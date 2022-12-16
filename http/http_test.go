package http

import (
	"context"
	"testing"

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

	server.http.ListenAndServe()
	client.Send("localhost:8080", rootId)

	// Figure out how to close down
}
