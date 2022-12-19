package http

import (
	"context"
	"testing"
	"time"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/util"
)

const MOCK_ID_HASH = 2

func init() {
	filter.RegisterHash(MOCK_ID_HASH, mock.XX3HashBlockId)
}

func assertBytesEqual(a []byte, b []byte, t *testing.T) {
	if len(a) != len(b) {
		t.Errorf("Length ase different: %v, %v", len(a), len(b))
	}
	compare_length := util.Min(len(a), len(b))
	var j int
	for j = 0; j < compare_length && a[j] == b[j]; j++ {
	}
	if j < compare_length {
		t.Errorf("First difference is at byte: %v", j)
	}
}

func TestClientSend(t *testing.T) {

	config := Config{
		MaxBatchSize:  20,
		Address:       ":8021",
		BloomCapacity: 1024,
		BloomFunction: MOCK_ID_HASH,
	}

	serverStore := mock.NewStore()
	clientStore := mock.NewStore()

	rootId := mock.AddRandomTree(context.Background(), clientStore, 12, 5, 0.0)

	server := NewServer[mock.BlockId](serverStore, config)
	client := NewClient[mock.BlockId](clientStore, config)

	errChan := make(chan error)

	go func() { errChan <- server.http.ListenAndServe() }()

	// Give the server time to start up
	time.Sleep(100 * time.Millisecond)

	client.Send("http://localhost:8021", rootId)
	client.CloseSource("http://localhost:8021") // will close session when finished

	var err error
	var info core.SenderSessionInfo[core.BatchState]
	// Wait for the session to go away
	for info, err = client.SourceInfo("http://localhost:8021"); err == nil; {
		log.Debugf("client info: %s", info.String())
		time.Sleep(100 * time.Millisecond)
	}

	if err != ErrInvalidSession {
		t.Errorf("Closed with unexpected error %v", err)
	}

	server.http.Close()

	if err = <-errChan; err != nil {
		t.Errorf("Server closed with error %v", err)
	}
}
