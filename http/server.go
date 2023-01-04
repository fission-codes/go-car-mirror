package http

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fission-codes/go-car-mirror/util"
)

type SessionToken string

type ServerSourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ServerSenderConnection[I, R]
	Session    *core.SourceSession[I, core.BatchState]
}

func NewServerSourceSessionData[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I], instrumented bool) *ServerSourceSessionData[I, R] {
	connection := NewServerSenderConnection[I, R](maxBatchSize)

	var orchestrator core.Orchestrator[core.BatchState] = core.NewBatchSourceOrchestrator()

	if instrumented {
		orchestrator = stats.NewInstrumentedOrchestrator[core.BatchState](orchestrator, stats.GLOBAL_STATS.WithContext("BatchSendOrchestrator"))
	}

	return &ServerSourceSessionData[I, R]{
		connection,
		core.NewSourceSession[I, core.BatchState](
			store,
			filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(allocator)),
			orchestrator,
		),
	}
}

type ServerSinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ServerReceiverConnection[I, R]
	Session    *core.SinkSession[I, core.BatchState]
}

func NewServerSinkSessionData[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I], instrumented bool) *ServerSinkSessionData[I, R] {
	connection := NewServerReceiverConnection[I, R](maxBatchSize)

	var orchestrator core.Orchestrator[core.BatchState] = core.NewBatchSinkOrchestrator()

	if instrumented {
		orchestrator = stats.NewInstrumentedOrchestrator[core.BatchState](orchestrator, stats.GLOBAL_STATS.WithContext("BatchReceiveOrchestrator"))
	}

	return &ServerSinkSessionData[I, R]{
		connection,
		core.NewSinkSession[I](
			store,
			core.NewSimpleStatusAccumulator(allocator()),
			orchestrator,
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
	instrumented   bool
}

func NewServer[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Server[I, R] {
	server := &Server[I, R]{
		store,
		util.NewSynchronizedMap[SessionToken, *ServerSourceSessionData[I, R]](),
		util.NewSynchronizedMap[SessionToken, *ServerSinkSessionData[I, R]](),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
		nil,
		config.Instrument,
	}

	mux := http.NewServeMux()

	mux.Handle("/", http.NotFoundHandler())

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

// Start starts the server
func (srv *Server[I, R]) Start() error {
	log.Debugw("enter", "object", "Server", "method", "Start")
	return srv.http.ListenAndServe()
}

// Stop stops the server
func (srv *Server[I, R]) Stop() error {
	log.Debugw("enter", "object", "Server", "method", "Stop")
	return srv.http.Close()
}

func (srv *Server[I, R]) generateToken(remoteAddr string) SessionToken {
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
	log.Debugw("Server", "method", "HandleStatus")
	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionToken SessionToken
	sourceCookie, err := request.Cookie("sourceSessionToken")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			log.Debugw("generating cookie", "object", "Server", "method", "HandleStatus")
			sessionToken = srv.generateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{
				Name:     "sourceSessionToken",
				Value:    (string)(sessionToken),
				Secure:   false,
				SameSite: http.SameSiteDefaultMode,
				MaxAge:   0,
			})
		default:
			log.Errorw("could not retrieve cookie", "object", "Server", "method", "HandleStatus", "error", err)
			http.Error(response, "server error", http.StatusInternalServerError)
			return
		}
	} else {
		sessionToken = SessionToken(sourceCookie.Value)
	}
	log.Debugw("have session token", "object", "Server", "method", "HandleStatus", "token", sessionToken)

	// Find the session from the session token, or create one
	sourceSession, ok := srv.sourceSessions.Get(sessionToken)
	if !ok {
		log.Debugw("no session found for token", "object", "Server", "method", "HandleStatus", "session", sessionToken)
		sourceSession = NewServerSourceSessionData[I, R](srv.store, srv.maxBatchSize, srv.allocator, srv.instrumented)
		srv.sourceSessions.Add(sessionToken, sourceSession)
		// Start the new session running; when it stops, remove it from the session map
		go func() {
			err := sourceSession.Session.Run(sourceSession.Connection)
			if err != nil {
				log.Errorw("session returned error", "object", "Server", "method", "HandleStatus", "session", sessionToken, "error", err)
			}
			srv.sourceSessions.Remove(sessionToken)
		}()
	}
	log.Debugw("have session for token", "object", "Server", "method", "HandleStatus", "session", sessionToken)

	// Parse the request to get the status message
	message := messages.StatusMessage[I, R, core.BatchState]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != nil {
		log.Errorw("parsing status message", "object", "Server", "method", "HandleStatus", "session", sessionToken, "error", err)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}

	// Send the status message to the session
	sourceSession.Session.HandleStatus(message.Have.Any(), message.Want)
	sourceSession.Session.HandleState(message.State)
	request.Body.Close()

	// Wait for a response from the session
	log.Debugw("waiting on session", "object", "Server", "method", "HandleStatus", "session", sessionToken)
	blocks := <-sourceSession.Connection.ResponseChannel()
	log.Debugw("session returned blocks", "object", "Server", "method", "HandleStatus", "len", len(blocks.Car.Blocks))

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	err = blocks.Write(response)

	if err != nil {
		log.Errorf("unexpected error writing response", "object", "Server", "method", "HandleStatus", "error", err)
	}
	log.Debugw("exit", "object", "Server", "method", "HandleStatus")
}

func (srv *Server[I, R]) HandleBlocks(response http.ResponseWriter, request *http.Request) {
	log.Debugw("enter", "object", "Server", "method", "HandleBlocks")
	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionToken SessionToken
	sinkCookie, err := request.Cookie("sinkSessionToken")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			log.Debugw("generating cookie", "object", "Server", "method", "HandleBlocks")
			sessionToken = srv.generateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{
				Name:     "sinkSessionToken",
				Value:    (string)(sessionToken),
				Secure:   false,
				SameSite: http.SameSiteDefaultMode,
				MaxAge:   0,
			})
		default:
			log.Errorw("could not retrieve cookie", "object", "Server", "method", "HandleBlocks", "error", err)
			http.Error(response, "server error", http.StatusInternalServerError)
			return
		}
	} else {
		sessionToken = SessionToken(sinkCookie.Value)
	}
	log.Debugw("have session token", "object", "Server", "method", "HandleBlocks", "token", sessionToken)

	// Find the session from the session token, or create one
	// TODO: Use GetOrInsert with a closure to avoid potentially spinning off sessions that are never closed
	sinkSession, ok := srv.sinkSessions.Get(sessionToken)
	if !ok {
		log.Debugw("no session found for token", "object", "Server", "method", "HandleBlocks", "session", sessionToken)
		sinkSession = NewServerSinkSessionData[I, R](srv.store, srv.maxBatchSize, srv.allocator, srv.instrumented)
		srv.sinkSessions.Add(sessionToken, sinkSession)
		// Start the new session running; when it stops, remove it from the session map
		go func() {
			err := sinkSession.Session.Run(sinkSession.Connection)
			if err != nil {
				log.Errorw("session returned error", "object", "Server", "method", "HandleBlocks", "session", sessionToken, "error", err)
			}
			srv.sourceSessions.Remove(sessionToken)
		}()

	}
	log.Debugw("have session for token", "object", "Server", "method", "HandleBlocks", "session", sessionToken)

	// Parse the request to get the blocks message
	message := messages.BlocksMessage[I, R, core.BatchState]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != io.EOF {
		log.Errorw("parsing blocks message", "object", "Server", "method", "HandleBlocks", "session", sessionToken, "error", err)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}
	request.Body.Close()

	log.Debugw("processed blocks", "object", "Server", "method", "HandleBlocks", "session", sessionToken, "count", len(message.Car.Blocks))

	// Send the blocks to the sessions
	receiver := core.NewSimpleBatchBlockReceiver[I](sinkSession.Session, sinkSession.Session.Orchestrator())
	err = receiver.HandleList(message.State, message.Car.Blocks)
	if err != nil {
		log.Errorw("could not handle block list", "object", "server", "method", "HandleBlocks", "session", sessionToken, "error", err)
	}

	// Wait for a response from the session
	log.Debugw("waiting on session", "object", "Server", "method", "HandleBlocks", "session", sessionToken)
	status := <-sinkSession.Connection.ResponseChannel()
	log.Debugw("session returned status", "object", "Server", "method", "HandleBlocks", "status", status)

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	err = status.Write(response)
	if err != nil {
		log.Errorf("unexpected error writing response", "object", "Server", "method", "HandleBlocks", "error", err)
	}
	log.Debugw("exit", "object", "Server", "method", "HandleBlocks")
}

func (srv *Server[I, R]) SourceSessions() []SessionToken {
	return srv.sourceSessions.Keys()
}

func (srv *Server[I, R]) SourceInfo(token SessionToken) (*core.SourceSessionInfo[core.BatchState], error) {
	if session, ok := srv.sourceSessions.Get(token); ok {
		return session.Session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (srv *Server[I, R]) SinkSessions() []SessionToken {
	return srv.sinkSessions.Keys()
}

func (srv *Server[I, R]) SinkInfo(token SessionToken) (*core.SinkSessionInfo[core.BatchState], error) {
	if session, ok := srv.sinkSessions.Get(token); ok {
		return session.Session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}
