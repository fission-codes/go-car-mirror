package http

import (
	"net/http"
	"net/http/cookiejar"

	"github.com/fission-codes/go-car-mirror/batch"
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/stats"
	"github.com/fission-codes/go-car-mirror/util"
)

func init() {
	stats.InitDefault()
}

type Client[I core.BlockId, R core.BlockIdRef[I]] struct {
	store          core.BlockStore[I]
	sourceSessions *util.SynchronizedMap[string, *core.SourceSession[I, batch.BatchState]]
	sinkSessions   *util.SynchronizedMap[string, *core.SinkSession[I, batch.BatchState]]
	maxBatchSize   uint32
	allocator      func() filter.Filter[I]
	instrumented   instrumented.InstrumentationOptions
}

func NewClient[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Client[I, R] {
	return &Client[I, R]{
		store,
		util.NewSynchronizedMap[string, *core.SourceSession[I, batch.BatchState]](),
		util.NewSynchronizedMap[string, *core.SinkSession[I, batch.BatchState]](),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
		config.Instrument,
	}
}

func (c *Client[I, R]) startSourceSession(url string) *core.SourceSession[I, batch.BatchState] {

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	sourceConnection := NewHttpClientSourceConnection[I, R](
		&http.Client{Jar: jar},
		url+"/dag/cm/blocks",
		stats.GLOBAL_STATS.WithContext(url),
		c.instrumented,
	)

	newSession := sourceConnection.Session(
		c.store,
		filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(c.allocator)),
	)

	newSender := sourceConnection.ImmediateSender(newSession, c.maxBatchSize)

	go func() {
		log.Debugw("starting source session", "object", "Client", "method", "startSourceSession", "url", url)
		newSession.Run(newSender)
		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSourceSession returns)
		c.sourceSessions.Remove(url)
		log.Debugw("source session ended", "object", "Client", "method", "startSourceSession", "url", url)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return newSession
}

func (c *Client[I, R]) GetSourceSession(url string) *core.SourceSession[I, batch.BatchState] {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session
	} else {
		session = c.sourceSessions.GetOrInsert(
			url,
			func() *core.SourceSession[I, batch.BatchState] {
				return c.startSourceSession(url)
			},
		)
		return session
	}
}

func (c *Client[I, R]) startSinkSession(url string) *core.SinkSession[I, batch.BatchState] {

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	sinkConnection := NewHttpClientSinkConnection[I, R](
		&http.Client{Jar: jar},
		url+"/dag/cm/status",
		stats.GLOBAL_STATS.WithContext(url),
		c.instrumented,
	)

	newSession := sinkConnection.Session(
		c.store,
		core.NewSimpleStatusAccumulator(c.allocator()),
	)

	sender := sinkConnection.ImmediateSender(newSession)

	go func() {
		log.Debugw("starting sink session", "object", "Client", "method", "startSinkSession", "url", url)
		newSession.Run(sender)

		// TODO: potential race condition if Run() completes before the
		// session is added to the list of sink sessions (which happens
		// when startSinkSession returns)
		c.sinkSessions.Remove(url)
		log.Debugw("ended sink session", "object", "Client", "method", "startSinkSession", "url", url)
	}()

	// Wait for the session to start
	<-newSession.Started()

	return newSession
}

func (c *Client[I, R]) GetSinkSession(url string) *core.SinkSession[I, batch.BatchState] {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session
	} else {
		session = c.sinkSessions.GetOrInsert(
			url,
			func() *core.SinkSession[I, batch.BatchState] {
				return c.startSinkSession(url)
			},
		)
		return session
	}
}

func (c *Client[I, R]) SourceSessions() []string {
	return c.sourceSessions.Keys()
}

func (c *Client[I, R]) SourceInfo(url string) (*core.SourceSessionInfo[batch.BatchState], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (c *Client[I, R]) SinkSessions() []string {
	return c.sinkSessions.Keys()
}

func (c *Client[I, R]) SinkInfo(url string) (*core.SinkSessionInfo[batch.BatchState], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

// TODO: This is only used in test now.  Remove?
func (c *Client[I, R]) Send(url string, id I) error {
	session := c.GetSourceSession(url)
	return session.Enqueue(id)
}

// TODO: This is only used in test now.  Remove?
func (c *Client[I, R]) Receive(url string, id I) error {
	session := c.GetSinkSession(url)
	return session.Enqueue(id)
}

func (c *Client[I, R]) CloseSource(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CloseSource", "url", url)
	session := c.GetSourceSession(url)
	err := session.Close()
	log.Debugw("exit", "object", "Client", "method", "CloseSource", "err", err)
	return err
}

func (c *Client[I, R]) CloseSink(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CloseSink", "url", url)
	session := c.GetSinkSession(url)
	err := session.Close()
	log.Debugw("exit", "object", "Client", "method", "CloseSink", "err", err)
	return err
}

// CancelSource cancels the source session with the given URL.
func (c *Client[I, R]) CancelSource(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CancelSource", "url", url)
	session := c.GetSourceSession(url)
	err := session.Cancel()
	log.Debugw("exit", "object", "Client", "method", "CancelSource", "err", err)
	return err
}

// CancelSink cancels the sink session with the given URL.
func (c *Client[I, R]) CancelSink(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CancelSink", "url", url)
	session := c.GetSinkSession(url)
	err := session.Cancel()
	log.Debugw("exit", "object", "Client", "method", "CancelSink", "err", err)
	return err
}
