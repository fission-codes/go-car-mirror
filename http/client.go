package http

import (
	"net/http"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
)

func init() {
	core.InitDefault()
}

type ClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientSenderConnection[I, R]
	Session    *core.SenderSession[I, core.BatchState]
}

func NewClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ClientSourceSessionData[I, R] {

	session := core.NewSenderSession[I, core.BatchState](
		store,
		filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(allocator)),
		core.NewBatchSendOrchestrator(),
	)

	connection := NewClientSenderConnection[I, R](maxBatchSize, &http.Client{}, target, session)

	return &ClientSourceSessionData[I, R]{
		connection,
		session,
	}
}

type ClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientReceiverConnection[I, R]
	Session    *core.ReceiverSession[I, core.BatchState]
}

func NewClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ClientSinkSessionData[I, R] {

	session := core.NewReceiverSession[I, core.BatchState](
		store,
		core.NewSimpleStatusAccumulator(allocator()),
		core.NewBatchReceiveOrchestrator(),
	)

	receiver := core.NewSimpleBatchBlockReceiver[I](session)

	connection := NewClientReceiverConnection[I, R](&http.Client{}, target, receiver)

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
}

func NewClient[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Client[I, R] {
	return &Client[I, R]{
		store,
		util.NewSynchronizedMap[string, *ClientSourceSessionData[I, R]](),
		util.NewSynchronizedMap[string, *ClientSinkSessionData[I, R]](),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
	}
}

func (c *Client[I, R]) startSourceSession(url string) *ClientSourceSessionData[I, R] {
	newSession := NewClientSourceSessionData[I, R](
		url+"/dag/cm/blocks",
		c.store,
		c.maxBatchSize,
		c.allocator,
	)
	go func() {
		log.Debugw("Client - starting source session", "url", url)
		if err := newSession.Session.Run(newSession.Connection); err != nil {
			log.Errorf("source session ended with error %v", err)
		}
		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSinkSession returns)
		c.sourceSessions.Remove(url)
		log.Debugw("ended source session")
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
	)
	go func() {
		if err := newSession.Session.Run(newSession.Connection); err != nil {
			log.Errorf("sink session ended with error %v", err)
		}
		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSinkSession returns)
		c.sinkSessions.Remove(url)
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

func (c *Client[I, R]) SourceInfo(url string) (core.SenderSessionInfo[core.BatchState], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session.Session.GetInfo(), nil
	} else {
		return core.SenderSessionInfo[core.BatchState]{}, ErrInvalidSession
	}
}

func (c *Client[I, R]) SinkSessions() []string {
	return c.sinkSessions.Keys()
}

func (c *Client[I, R]) SinkInfo(url string) (core.ReceiverSessionInfo[core.BatchState], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session.Session.GetInfo(), nil
	} else {
		return core.ReceiverSessionInfo[core.BatchState]{}, ErrInvalidSession
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
	_, err := c.GetSinkSession(url)
	if err != nil {
		return err
	}
	// TODO: Need something here
	return nil
}

func (c *Client[I, R]) CloseSource(url string) error {
	session, err := c.GetSourceSession(url)
	if err != nil {
		return err
	}
	session.Session.Close()
	return nil
}

func (c *Client[I, R]) CloseSink(url string) error {
	session, err := c.GetSinkSession(url)
	if err != nil {
		return err
	}
	session.Session.Cancel() // TODO: Implement graceful shutdown from Sink
	return nil
}
