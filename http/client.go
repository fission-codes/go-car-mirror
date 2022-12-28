package http

import (
	"net/http"
	"net/http/cookiejar"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fission-codes/go-car-mirror/util"
)

func init() {
	stats.InitDefault()
}

type ClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientSenderConnection[I, R]
	Session    *core.SenderSession[I, core.BatchState]
}

func NewClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I], instrumented bool) *ClientSourceSessionData[I, R] {

	var orchestrator core.Orchestrator[core.BatchState] = core.NewBatchSendOrchestrator()

	if instrumented {
		orchestrator = stats.NewInstrumentedOrchestrator[core.BatchState](orchestrator, stats.GLOBAL_STATS.WithContext("BatchSendOrchestrator"))
	}

	session := core.NewSenderSession[I, core.BatchState](
		store,
		filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(allocator)),
		orchestrator,
	)

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	connection := NewClientSenderConnection[I, R](
		maxBatchSize,
		&http.Client{Jar: jar},
		target,
		session,
	)

	return &ClientSourceSessionData[I, R]{
		connection,
		session,
	}
}

type ClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientReceiverConnection[I, R]
	Session    *core.ReceiverSession[I, core.BatchState]
}

func NewClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I], instrumented bool) *ClientSinkSessionData[I, R] {

	var orchestrator core.Orchestrator[core.BatchState] = core.NewBatchReceiveOrchestrator()

	if instrumented {
		orchestrator = stats.NewInstrumentedOrchestrator[core.BatchState](orchestrator, stats.GLOBAL_STATS.WithContext("BatchReceiveOrchestrator"))
	}

	session := core.NewReceiverSession[I, core.BatchState](
		store,
		core.NewSimpleStatusAccumulator(allocator()),
		orchestrator,
	)

	receiver := core.NewSimpleBatchBlockReceiver[I](session, orchestrator)

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	connection := NewClientReceiverConnection[I, R](&http.Client{Jar: jar}, target, receiver)

	return &ClientSinkSessionData[I, R]{
		connection,
		session,
	}
}

type Client[I core.BlockId, R core.BlockIdRef[I]] struct {
	store          core.BlockStore[I]
	sourceSessions *util.SynchronizedMap[string, *ClientSourceSessionData[I, R]]
	sinkSessions   *util.SynchronizedMap[string, *ClientSinkSessionData[I, R]]
	maxBatchSize   uint32
	allocator      func() filter.Filter[I]
	instrumented   bool
}

func NewClient[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Client[I, R] {
	return &Client[I, R]{
		store,
		util.NewSynchronizedMap[string, *ClientSourceSessionData[I, R]](),
		util.NewSynchronizedMap[string, *ClientSinkSessionData[I, R]](),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
		config.Instrument,
	}
}

func (c *Client[I, R]) startSourceSession(url string) *ClientSourceSessionData[I, R] {
	newSession := NewClientSourceSessionData[I, R](
		url+"/dag/cm/blocks",
		c.store,
		c.maxBatchSize,
		c.allocator,
		c.instrumented,
	)
	go func() {
		log.Debugw("starting source session", "object", "Client", "method", "startSourceSession", "url", url)
		if err := newSession.Session.Run(newSession.Connection); err != nil {
			log.Errorw("source session ended with error", "object", "Client", "method", "startSourceSession", "url", url, "error", err)
		}
		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSourceSession returns)
		c.sourceSessions.Remove(url)
		log.Debugw("source session ended", "object", "Client", "method", "startSourceSession", "url", url)
	}()

	return newSession
}

func (c *Client[I, R]) GetSourceSession(url string) (*ClientSourceSessionData[I, R], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sourceSessions.GetOrInsert(
			url,
			func() *ClientSourceSessionData[I, R] {
				return c.startSourceSession(url)
			},
		)
		return session, nil
	}
}

func (c *Client[I, R]) startSinkSession(url string) *ClientSinkSessionData[I, R] {
	newSession := NewClientSinkSessionData[I, R](
		url+"/dag/cm/status",
		c.store,
		c.maxBatchSize,
		c.allocator,
		c.instrumented,
	)
	go func() {
		log.Debugw("starting sink session", "object", "Client", "method", "startSinkSession", "url", url)
		if err := newSession.Session.Run(newSession.Connection); err != nil {
			log.Errorw("sink session ended with error", "object", "Client", "method", "startSinkSession", "url", url, "error", err)
		}
		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSinkSession returns)
		c.sinkSessions.Remove(url)
		log.Debugw("ended sink session", "object", "Client", "method", "startSinkSession", "url", url)
	}()

	return newSession
}

func (c *Client[I, R]) GetSinkSession(url string) (*ClientSinkSessionData[I, R], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sinkSessions.GetOrInsert(
			url,
			func() *ClientSinkSessionData[I, R] {
				return c.startSinkSession(url)
			},
		)
		return session, nil
	}
}

func (c *Client[I, R]) SourceSessions() []string {
	return c.sourceSessions.Keys()
}

func (c *Client[I, R]) SourceInfo(url string) (*core.SenderSessionInfo[core.BatchState], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session.Session.GetInfo(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (c *Client[I, R]) SinkSessions() []string {
	return c.sinkSessions.Keys()
}

func (c *Client[I, R]) SinkInfo(url string) (*core.ReceiverSessionInfo[core.BatchState], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session.Session.GetInfo(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (c *Client[I, R]) Send(url string, id I) error {
	session, err := c.GetSourceSession(url)
	if err != nil {
		return err
	}
	session.Session.Enqueue(id)
	return nil
}

func (c *Client[I, R]) Receive(url string, id I) error {
	session, err := c.GetSinkSession(url)
	if err != nil {
		return err
	}
	return session.Session.AccumulateStatus(id)
}

func (c *Client[I, R]) CloseSource(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CloseSource", "url", url)
	session, err := c.GetSourceSession(url)
	if err != nil {
		log.Debugw("exit", "object", "Client", "method", "CloseSource", "err", err)
		return err
	}
	err = session.Session.Close()
	log.Debugw("exit", "object", "Client", "method", "CloseSource", "err", err)
	return err
}

func (c *Client[I, R]) CloseSink(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CloseSink", "url", url)
	session, err := c.GetSinkSession(url)
	if err != nil {
		log.Debugw("exit", "object", "Client", "method", "CloseSink", "err", err)
		return err
	}
	err = session.Session.Close()
	log.Debugw("exit", "object", "Client", "method", "CloseSink", "err", err)
	return err
}
