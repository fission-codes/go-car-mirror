package http

import (
	"bufio"
	"errors"
	"net/http"
	"time"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/messages"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("server")

type SessionToken string

type Config struct {
	MaxBatchSize  uint32
	Address       string
	BloomCapacity uint
	BloomFunction uint64
}

func DefaultConfig() Config {
	return Config{
		MaxBatchSize: 32,
		Address:      ":8080",
	}
}

func NewBloomAllocator[I core.BlockId](config *Config) func() filter.Filter[I] {
	return func() filter.Filter[I] {
		filter, err := filter.TryNewBloomFilter[I](config.BloomCapacity, config.BloomFunction)
		if err != nil {
			log.Errorf("Invalid hash function specified %v", config.BloomFunction)
		}
		return filter
	}
}

type Server[I core.BlockId, R core.BlockIdRef[I]] struct {
	store          core.BlockStore[I]
	sourceSessions map[SessionToken]*core.SenderSession[I, core.BatchState]
	sinkSessions   map[SessionToken]*core.ReceiverSession[I, core.BatchState]
	maxBatchSize   uint32
	allocator      func() filter.Filter[I]
	http           *http.Server
}

func NewServer[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Server[I, R] {
	server := &Server[I, R]{
		store,
		make(map[SessionToken]*core.SenderSession[I, core.BatchState]),
		make(map[SessionToken]*core.ReceiverSession[I, core.BatchState]),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
		nil,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/dag/cm/status", func(response http.ResponseWriter, request *http.Request) {
		server.HandleStatus(response, request)
	})

	mux.HandleFunc("/dag/cm/blocks", func(response http.ResponseWriter, request *http.Request) {
		server.HandleBlocks(response, request)
	})

	server.http = &http.Server{
		Addr:           config.Address,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	return server
}

func (srv *Server[I, R]) HandleStatus(response http.ResponseWriter, request *http.Request) {
	sourceCookie, err := request.Cookie("sourceCookie")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			http.Error(response, "cookie not found", http.StatusBadRequest)
		default:
			log.Errorf("unexpected error: %v", err)
			http.Error(response, "server error", http.StatusInternalServerError)
		}
		return
	}

	sourceSession, ok := srv.sourceSessions[SessionToken(sourceCookie.Value)]
	if !ok {
		sourceSession = core.NewSenderSession[I, core.BatchState](
			srv.store,
			NewServerSenderConnection[I, R](srv.maxBatchSize),
			filter.NewSynchronizedFilter[I](filter.NewEmptyFilter[I](srv.allocator)),
			core.NewBatchSendOrchestrator(),
		)
		// TODO: synchronize sourceSessions
		// TODO: add sourceSession to map
		// TODO: figure out how to provide access to the response channel
	}

	message := messages.StatusMessage[I, R, core.BatchState]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != nil {
		log.Errorf("could not parse state message for: %v", sourceCookie)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}

	sourceSession.HandleStatus(message.Have.Any(), message.Want)
	sourceSession.HandleState(message.State)
	sourceSession.
		request.Body.Close()

	response.WriteHeader(http.StatusAccepted)
}

func (*Server[I]) HandleBlocks(response http.ResponseWriter, request *http.Request) {

}
