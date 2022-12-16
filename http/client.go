package http

import (
	"net/http"

	core "github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
)

type ClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientSenderConnection[I, R]
	Session    *core.SenderSession[I, core.BatchState]
}

func NewClientSourceSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ClientSourceSessionData[I, R] {

	connection := NewClientSenderConnection[I, R](maxBatchSize, &http.Client{}, target)

	return &ClientSourceSessionData[I, R]{
		connection,
		core.NewSenderSession[I, core.BatchState](
			store,
			connection,
			filter.NewSynchronizedFilter[I](filter.NewEmptyFilter[I](allocator)),
			core.NewBatchSendOrchestrator(),
		),
	}
}

type ClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]] struct {
	Connection *ClientReceiverConnection[I, R]
	Session    *core.ReceiverSession[I, core.BatchState]
}

func NewClientSinkSessionData[I core.BlockId, R core.BlockIdRef[I]](target string, store core.BlockStore[I], maxBatchSize uint32, allocator func() filter.Filter[I]) *ClientSinkSessionData[I, R] {

	connection := NewClientReceiverConnection[I, R](&http.Client{}, target)

	return &ClientSinkSessionData[I, R]{
		connection,
		core.NewReceiverSession[I, core.BatchState](
			store,
			connection,
			core.NewSimpleStatusAccumulator(allocator()),
			core.NewBatchReceiveOrchestrator(),
		),
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

func (c *Client[I, R]) GetSourceSession(url string) (*ClientSourceSessionData[I, R], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sourceSessions.GetOrInsert(
			url,
			NewClientSourceSessionData[I, R](
				url+"/cm/blocks",
				c.store,
				c.maxBatchSize,
				c.allocator,
			),
		)
		return session, nil
	}
}

func (c *Client[I, R]) GetSinkSession(url string) (*ClientSinkSessionData[I, R], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sinkSessions.GetOrInsert(
			url,
			NewClientSinkSessionData[I, R](
				url+"/cm/status",
				c.store,
				c.maxBatchSize,
				c.allocator,
			),
		)
		return session, nil
	}
}

func (c *Client[I, R]) SourceSessions() []string {
	return c.sourceSessions.Keys()
}

func (c *Client[I, R]) SinkSessions() []string {
	return c.sinkSessions.Keys()
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
