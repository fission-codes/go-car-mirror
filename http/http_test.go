package http

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/fission-codes/go-car-mirror/batch"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/stats"
)

const MOCK_ID_HASH = 2

func init() {
	filter.RegisterHash(MOCK_ID_HASH, mock.XX3HashBlockId)
}

func TestClientSend(t *testing.T) {
	// t.Skip()
	serverConfig := Config{
		Address: ":8021",
	}
	responderConfig := batch.Config{
		MaxBatchSize:  100,
		BloomCapacity: 256,
		BloomFunction: MOCK_ID_HASH,
		Instrument:    instrumented.INSTRUMENT_STORE | instrumented.INSTRUMENT_ORCHESTRATOR,
	}

	serverStore := mock.NewStore(mock.Config{})
	clientStore := mock.NewStore(mock.Config{})

	rootId := mock.AddRandomTree(context.Background(), clientStore, 12, 5, 0.0)

	server := NewServer[mock.BlockId](serverStore, serverConfig, responderConfig)
	client := NewClient[mock.BlockId](clientStore, responderConfig)

	errChan := make(chan error)

	go func() { errChan <- server.Start() }()

	// Give the server time to start up
	time.Sleep(100 * time.Millisecond)

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	// TODO: get session and enqueue before starting.
	url := "http://localhost:8021"
	client.Send(url, rootId)

	go func() {
		log.Debugf("timeout started")
		time.Sleep(10 * time.Second)
		log.Debugf("timeout elapsed")

		// If we timeout, see what goroutines are hung
		// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		if err := client.GetSourceSession(url).Cancel(); err != nil {
			log.Errorf("error cancelling sender session: %v", err)
		}

		server.Stop()
	}()

	// Wait for the session to go away
	info, err := client.SourceInfo(url)
	for err == nil {
		log.Debugf("client info: %s", info.String())
		time.Sleep(100 * time.Millisecond)
		info, err = client.SourceInfo(url)
	}

	if err != ErrInvalidSession {
		t.Errorf("Closed with unexpected error %v", err)
	}

	server.Stop()

	if err = <-errChan; err != http.ErrServerClosed && err != nil {
		t.Errorf("Server closed with error %v", err)
	}

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)

	if !serverStore.HasAll(rootId) {
		t.Errorf("Expected server store to have all children of %v", rootId)
	}
}

func TestClientReceive(t *testing.T) {
	// t.Skip("TODO")
	serverConfig := Config{
		Address: ":8021",
	}
	responderConfig := batch.Config{
		MaxBatchSize:  100,
		BloomCapacity: 256,
		BloomFunction: MOCK_ID_HASH,
		Instrument:    instrumented.INSTRUMENT_STORE | instrumented.INSTRUMENT_ORCHESTRATOR,
	}

	serverStore := mock.NewStore(mock.Config{})
	clientStore := mock.NewStore(mock.Config{})

	rootId := mock.AddRandomTree(context.Background(), serverStore, 2, 2, 0.0)

	server := NewServer[mock.BlockId](serverStore, serverConfig, responderConfig)
	client := NewClient[mock.BlockId](clientStore, responderConfig)

	errChan := make(chan error)

	go func() { errChan <- server.Start() }()

	// Give the server time to start up
	time.Sleep(100 * time.Millisecond)

	snapshotBefore := stats.GLOBAL_REPORTING.Snapshot()

	url := "http://localhost:8021"
	session := client.GetSinkSession(url)
	go func() {
		if err := session.Enqueue(rootId); err != nil {
			log.Debugw("error enqueuing root", "err", err)
			return
		}
	}()

	go func() {
		log.Debugf("timeout started")
		time.Sleep(10 * time.Second)
		log.Debugf("timeout elapsed")

		// If we timeout, see what goroutines are hung
		// pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)

		if err := session.Cancel(); err != nil {
			log.Errorf("error cancelling sender session: %v", err)
		}

		// server.Stop()
	}()

	// Wait for the session to go away
	err := <-session.Done()

	if err != ErrInvalidSession {
		t.Errorf("Closed with unexpected error %v", err)
	}

	server.Stop()

	if err = <-errChan; err != http.ErrServerClosed && err != nil {
		t.Errorf("Server closed with error %v", err)
	}

	snapshotAfter := stats.GLOBAL_REPORTING.Snapshot()
	diff := snapshotBefore.Diff(snapshotAfter)
	diff.Write(&log.SugaredLogger)

	if !clientStore.HasAll(rootId) {
		t.Errorf("Expected client store to have all children of %v", rootId)
		clientStore.Dump(rootId, &log.SugaredLogger, "")
	}
}
