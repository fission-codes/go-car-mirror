package http

import (
	"context"
	"net/http"
	"testing"
	"time"

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
		MaxBatchSize:  100,
		Address:       ":8021",
		BloomCapacity: 256,
		BloomFunction: MOCK_ID_HASH,
		Instrument:    false,
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

	// Wait for the session to go away
	info, err := client.SourceInfo("http://localhost:8021")
	for err == nil {
		log.Debugf("client info: %s", info.String())
		time.Sleep(100 * time.Millisecond)
		info, err = client.SourceInfo("http://localhost:8021")
	}

	if err != ErrInvalidSession {
		t.Errorf("Closed with unexpected error %v", err)
	}

	server.http.Close()

	if err = <-errChan; err != http.ErrServerClosed && err != nil {
		t.Errorf("Server closed with error %v", err)
	}

	if !serverStore.HasAll(rootId) {
		t.Errorf("Expected server store to have all children of %v", rootId)
	}
}
