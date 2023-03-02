package http

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/fission-codes/go-car-mirror/batch"
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/stats"
)

type Server[I core.BlockId, R core.BlockIdRef[I]] struct {
	store           core.BlockStore[I]
	maxBatchSize    uint32
	http            *http.Server
	instrumented    instrumented.InstrumentationOptions
	sinkResponder   *batch.SinkResponder[I]
	sourceResponder *batch.SourceResponder[I]
}

// TODO: change config to something more flexible, so it can work for responder and this server.
// We have server config and responder config, and they are different.
// Start with 2 configs I guess
func NewServer[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], serverConfig Config, responderConfig batch.Config) *Server[I, R] {
	server := &Server[I, R]{
		store,
		responderConfig.MaxBatchSize,
		nil,
		responderConfig.Instrument,
		// TODO: Set the batchStatusSender
		batch.NewSinkResponder[I](store, responderConfig, nil),
		// TODO: Set the batchBlockSender
		batch.NewSourceResponder[I](store, responderConfig, nil, responderConfig.MaxBatchSize),
	}

	sinkConn := NewHttpServerSinkConnection[I, R](stats.GLOBAL_STATS, responderConfig.Instrument)
	server.sinkResponder.SetStatusSender(sinkConn.DeferredSender())

	// sourceConn := NewHttpServerSourceConnection[I, R](stats.GLOBAL_STATS, responderConfig.Instrument)
	// blockSender := sourceConn.DeferredSender(responderConfig.MaxBatchSize)
	// server.sourceResponder.SetBatchBlockSender()

	mux := http.NewServeMux()

	mux.Handle("/", http.NotFoundHandler())

	mux.HandleFunc("/dag/cm/status", func(response http.ResponseWriter, request *http.Request) {
		server.HandleStatus(response, request)
	})

	mux.HandleFunc("/dag/cm/blocks", func(response http.ResponseWriter, request *http.Request) {
		server.HandleBlocks(response, request)
	})

	server.http = &http.Server{
		Addr:           serverConfig.Address,
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	// TODO
	// Call server.sinkResponder.SetStatusSender(...)
	// Call server.sourceResponder.SetBatchBlockSender(...)
	// Also make sure we can receive from the other side, whatever we pass into those.
	// This is taking the place of DeferredSender and PendingResponse.

	// For send we did:
	// senderSession := sourceConnection.Session(
	// 	senderStore,
	// 	filter.NewSynchronizedFilter(batch.NewBloomAllocator[mock.BlockId](&config)()),
	// 	true, // Requester
	// )
	// blockSender := sourceConnection.Sender(&blockChannel, uint32(maxBatchSize))
	// sinkResponder.SetStatusSender(&statusChannel)
	// statusChannel.SetReceiverFunc(func() *batch.SimpleBatchStatusReceiver[mock.BlockId] {
	//   return sourceConnection.Receiver(senderSession)
	// })

	// For receive we did:
	// receiverSession := sinkConnection.Session(
	// 	NewSynchronizedBlockStore[mock.BlockId](NewSynchronizedBlockStore[mock.BlockId](sinkStore)),
	// 	NewSimpleStatusAccumulator(batch.NewBloomAllocator[mock.BlockId](&config)()),
	// 	true, // Requester
	// )
	// statusSender := sinkConnection.Sender(&statusChannel)

	// sourceResponder.SetBatchBlockSender(&blockChannel)

	// blockChannel.SetReceiverFunc(func() batch.BatchBlockReceiver[mock.BlockId] {
	// 	return sinkConnection.Receiver(receiverSession)
	// })

	// statusChannel.SetReceiverFunc(func() *batch.SimpleBatchStatusReceiver[mock.BlockId] {
	// 	return sourceResponder.Receiver(SESSION_ID)
	// })

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

func (srv *Server[I, R]) generateToken(remoteAddr string) batch.SessionId {
	// We might at some stage do something to verify session tokens, but for now they are
	// just 128 bit random numbers
	token := make([]byte, 16)
	_, err := rand.Read(token)
	if err != nil {
		panic(err)
	}
	return batch.SessionId(base64.URLEncoding.EncodeToString(token))
}

func (srv *Server[I, R]) HandleStatus(response http.ResponseWriter, request *http.Request) {
	log.Debugw("Server", "method", "HandleStatus")
	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionId batch.SessionId
	sourceCookie, err := request.Cookie("sourceSessionId")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			log.Debugw("generating cookie", "object", "Server", "method", "HandleStatus")
			sessionId = srv.generateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{
				Name:     "sourceSessionId",
				Value:    (string)(sessionId),
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
		sessionId = batch.SessionId(sourceCookie.Value)
	}
	log.Debugw("have session token", "object", "Server", "method", "HandleStatus", "token", sessionId)

	// Parse the request to get the status message
	message := messages.StatusMessage[I, R]{}
	messageReader := bufio.NewReader(request.Body)
	if err := message.Read(messageReader); err != nil {
		log.Errorw("parsing status message", "object", "Server", "method", "HandleStatus", "session", sessionId, "error", err)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}

	if len(message.Want) == 0 {
		log.Debugw("received status message with no wants", "object", "Server", "method", "HandleStatus", "session", sessionId, "message", message)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}

	// TODO: Handle errors
	// sourceSession.conn.Receiver(sourceSession.Session).HandleStatus(message.State, message.Have.Any(), message.Want)
	if err := srv.sourceResponder.Receiver(sessionId).HandleStatus(message.Have.Any(), message.Want); err != nil {
		log.Errorw("handling status message", "object", "Server", "method", "HandleStatus", "session", sessionId, "error", err)
		http.Error(response, "server error", http.StatusInternalServerError)
		return
	}
	request.Body.Close()

	// Wait for a response from the session
	log.Debugw("waiting on session", "object", "Server", "method", "HandleStatus", "session", sessionId)
	// TODO: In the past this call hung due to reading from a channel with no sender.
	// blocks := srv.sourceResponder.SourceConnection(sessionId).PendingResponse()

	// blocks := sourceSession.conn.PendingResponse()
	// log.Debugw("session returned blocks", "object", "Server", "method", "HandleStatus", "len", len(blocks.Car.Blocks))

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	// err = blocks.Write(response)

	if err != nil {
		log.Errorf("unexpected error writing response", "object", "Server", "method", "HandleStatus", "error", err)
	}
	log.Debugw("exit", "object", "Server", "method", "HandleStatus")
}

func (srv *Server[I, R]) HandleBlocks(response http.ResponseWriter, request *http.Request) {
	log.Debugw("enter", "object", "Server", "method", "HandleBlocks")
	// First get a session token from a cookie; if no such cookie exists, create one
	var sessionId batch.SessionId
	sinkCookie, err := request.Cookie("sinkSessionId")
	if err != nil {
		switch {
		case errors.Is(err, http.ErrNoCookie):
			log.Debugw("generating cookie", "object", "Server", "method", "HandleBlocks")
			sessionId = srv.generateToken(request.RemoteAddr)
			http.SetCookie(response, &http.Cookie{
				Name:     "sinkSessionId",
				Value:    (string)(sessionId),
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
		sessionId = batch.SessionId(sinkCookie.Value)
	}
	log.Debugw("have session token", "object", "Server", "method", "HandleBlocks", "token", sessionId)

	// Parse the request to get the blocks message
	message := messages.BlocksMessage[I, R]{}
	messageReader := bufio.NewReader(request.Body)
	// TODO: i/o timeout here, leads to BadRequest being returned
	if err := message.Read(messageReader); err != io.EOF {
		log.Errorw("parsing blocks message", "object", "Server", "method", "HandleBlocks", "session", sessionId, "error", err)
		http.Error(response, "bad message format", http.StatusBadRequest)
		return
	}
	request.Body.Close()

	log.Debugw("processed blocks", "object", "Server", "method", "HandleBlocks", "session", sessionId, "count", len(message.Car.Blocks))

	// Send the blocks to the sessions
	// err = sinkSession.conn.Receiver(sinkSession.Session).HandleList(message.Car.Blocks)
	err = srv.sinkResponder.Receiver(sessionId).HandleList(message.Car.Blocks)
	if err != nil {
		log.Errorw("could not handle block list", "object", "server", "method", "HandleBlocks", "session", sessionId, "error", err)
	}

	// Wait for a response from the session
	log.Debugw("waiting on session", "object", "Server", "method", "HandleBlocks", "session", sessionId)
	// status := srv.sinkResponder.SinkConnection(sessionId).PendingResponse()
	// log.Debugw("session returned status", "object", "Server", "method", "HandleBlocks", "status", status)

	// Write the response
	response.WriteHeader(http.StatusAccepted)
	// err = status.Write(response)
	if err != nil {
		log.Errorf("unexpected error writing response", "object", "Server", "method", "HandleBlocks", "error", err)
	}
	log.Debugw("exit", "object", "Server", "method", "HandleBlocks")
}

func (srv *Server[I, R]) SourceSessions() []core.SourceSession[I, batch.BatchState] {
	return nil
}

func (srv *Server[I, R]) SinkSessions() []core.SinkSession[I, batch.BatchState] {
	return nil
}

func (srv *Server[I, R]) SinkInfo(token batch.SessionId) (*core.SinkSessionInfo[batch.BatchState], error) {
	if session := srv.sinkResponder.SinkSession(token); session != nil {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (srv *Server[I, R]) SourceInfo(token batch.SessionId) (*core.SourceSessionInfo[batch.BatchState], error) {
	if session := srv.sourceResponder.SourceSession(token); session != nil {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}
