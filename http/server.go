package http

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/util"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("http")

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

type ServerSourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ServerSenderConnection[I, R]
	Session    *core.SenderSession[I, core.BatchState]
}

func NewServerSourceSessionData[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ServerSourceSessionData[I, R] {
	connection := NewServerSenderConnection[I, R](maxBatchSize)
	return &ServerSourceSessionData[I, R]{
		connection,
		core.NewSenderSession[I, core.BatchState](
			store,
			connection,
			filter.NewSynchronizedFilter[I](filter.NewEmptyFilter[I](allocator)),
			core.NewBatchSendOrchestrator(),
		),
	}
}

type ServerSinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ServerReceiverConnection[I, R]
	Session    *core.ReceiverSession[I, core.BatchState]
}

func NewServerSinkSessionData[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ServerSinkSessionData[I, R] {
	connection := NewServerReceiverConnection[I, R](maxBatchSize)
	return &ServerSinkSessionData[I, R]{
		connection,
		core.NewReceiverSession[I, core.BatchState](
			store,
			connection,
			core.NewSimpleStatusAccumulator(allocator()),
			core.NewBatchReceiveOrchestrator(),
		),
	}
}

type Server[I core.BlockId, R core.BlockIdRef[I]] struct {
	store          core.BlockStore[I]
	sourceSessions *util.SynchronizedMap[SessionToken, *ServerSourceSessionData[I, R]]
	sinkSessions   *util.SynchronizedMap[SessionToken, *ServerSinkSessionData[I, R]]
	maxBatchSize   uint32
	allocator      func() filter.Filter[I]
	http           *http.Server
}

func NewServer[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Server[I, R] {
	server := &Server[I, R]{
		store,
		util.NewSynchronizedMap[SessionToken, *ServerSourceSessionData[I, R]](),
		util.NewSynchronizedMap[SessionToken, *ServerSinkSessionData[I, R]](),
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

func (srv *Server[I, R]) GenerateToken(remoteAddr string) SessionToken {
	// We might at some stage do something to verify session tokens, but for now they are
	// just 128 bit random numbers
	token := make([]byte, 16)
	_, err := rand.Read(token)
	if err != nil {
		panic(err)
	}
	return SessionToken(base64.URLEncoding.EncodeToString(token))
}

func (srv *Server[I, R]) HandleStatus(response http.ResponseWriter, request *http.Request) {

	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionToken SessionToken
	sourceCookie, err := request.Cookie("sourceSessionToken")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			sessionToken = srv.GenerateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{Name: "sourceSessionToken", Value: (string)(sessionToken)})
		default:
			log.Errorf("unexpected error: %v", err)
			http.Error(response, "server error", http.StatusInternalServerError)
			return
		}
	} else {
		sessionToken = SessionToken(sourceCookie.Value)
	}

	// Find the session from the session token, or create one
	sourceSession, ok := srv.sourceSessions.Get(sessionToken)
	if !ok {
		sourceSession = NewServerSourceSessionData[I, R](srv.store, srv.maxBatchSize, srv.allocator)
		srv.sourceSessions.Add(sessionToken, sourceSession)
	}

	// Parse the request to get the status message
	message := messages.StatusMessage[I, R, core.BatchState]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != nil {
		log.Errorf("could not parse state message for: %v", sessionToken)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}

	// Send the status message to the session
	sourceSession.Session.HandleStatus(message.Have.Any(), message.Want)
	sourceSession.Session.HandleState(message.State)
	request.Body.Close()

	// Wait for a response from the session
	blocks := <-sourceSession.Connection.ResponseChannel()

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	blocks.Write(response)
}

func (srv *Server[I, R]) HandleBlocks(response http.ResponseWriter, request *http.Request) {
	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionToken SessionToken
	sinkCookie, err := request.Cookie("sinkSessionToken")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			sessionToken = srv.GenerateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{Name: "sinkSessionToken", Value: (string)(sessionToken)})
		default:
			log.Errorf("unexpected error: %v", err)
			http.Error(response, "server error", http.StatusInternalServerError)
			return
		}
	} else {
		sessionToken = SessionToken(sinkCookie.Value)
	}

	// Find the session from the session token, or create one
	sinkSession, ok := srv.sinkSessions.Get(sessionToken)
	if !ok {
		sinkSession = NewServerSinkSessionData[I, R](srv.store, srv.maxBatchSize, srv.allocator)
		srv.sinkSessions.Add(sessionToken, sinkSession)
	}

	// Parse the request to get the blocks message
	message := messages.BlocksMessage[I, R, core.BatchState]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != nil {
		log.Errorf("could not parse blocks message for: %v", sessionToken)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}
	request.Body.Close()

	// Send the blocks to the sessions
	receiver := core.NewSimpleBatchBlockReceiver[I](sinkSession.Session)
	receiver.HandleList(message.State, message.Car.Blocks)

	// Wait for a response from the session
	status := <-sinkSession.Connection.ResponseChannel()

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	status.Write(response)
}
