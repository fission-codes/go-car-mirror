package http

import (
	"net/http"
	"net/http/cookiejar"

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
	sourceSessions *util.SynchronizedMap[string, *core.SourceSession[I, core.BatchState]]
	sinkSessions   *util.SynchronizedMap[string, *core.SinkSession[I, core.BatchState]]
	maxBatchSize   uint32
	allocator      func() filter.Filter[I]
	instrumented   instrumented.InstrumentationOptions
}

func NewClient[I core.BlockId, R core.BlockIdRef[I]](store core.BlockStore[I], config Config) *Client[I, R] {
	return &Client[I, R]{
		store,
		util.NewSynchronizedMap[string, *core.SourceSession[I, core.BatchState]](),
		util.NewSynchronizedMap[string, *core.SinkSession[I, core.BatchState]](),
		config.MaxBatchSize,
		NewBloomAllocator[I](&config),
		config.Instrument,
	}
}

func (c *Client[I, R]) startSourceSession(url string) *core.SourceSession[I, core.BatchState] {

	var orchestrator core.Orchestrator[core.BatchState] = core.NewBatchSourceOrchestrator()

	newSession := instrumented.NewSourceSession[I](
		c.store,
		filter.NewSynchronizedFilter[I](filter.NewEmptyFilter(c.allocator)),
		orchestrator,
		stats.GLOBAL_STATS.WithContext(url),
		c.instrumented,
	)

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	newReceiver := core.NewSimpleBatchStatusReceiver[I](newSession, newSession.Orchestrator())

	newSender := core.NewSimpleBatchBlockSender[I](
		&RequestBatchBlockSender[I, R]{&http.Client{Jar: jar}, url + "/dag/cm/blocks", newReceiver},
		newSession.Orchestrator(),
		c.maxBatchSize,
	)

	go func() {
		log.Debugw("starting source session", "object", "Client", "method", "startSourceSession", "url", url)
		if err := newSession.Run(newSender); err != nil {
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

func (c *Client[I, R]) GetSourceSession(url string) (*core.SourceSession[I, core.BatchState], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sourceSessions.GetOrInsert(
			url,
			func() *core.SourceSession[I, core.BatchState] {
				return c.startSourceSession(url)
			},
		)
		return session, nil
	}
}

func (c *Client[I, R]) startSinkSession(url string) *core.SinkSession[I, core.BatchState] {

	newSession := instrumented.NewSinkSession[I, core.BatchState](
		c.store,
		core.NewSimpleStatusAccumulator(c.allocator()),
		core.NewBatchSinkOrchestrator(),
		stats.GLOBAL_STATS.WithContext(url),
		c.instrumented,
	)

	jar, err := cookiejar.New(&cookiejar.Options{}) // TODO: set public suffix list
	if err != nil {
		panic(err)
	}

	sender := &RequestStatusSender[I, R]{
		newSession.Orchestrator(),
		&http.Client{Jar: jar}, //binks
		url + "/dag/cm/status",
		core.NewSimpleBatchBlockReceiver[I](newSession, newSession.Orchestrator()),
	}

	go func() {
		log.Debugw("starting sink session", "object", "Client", "method", "startSinkSession", "url", url)
		if err := newSession.Run(sender); err != nil {
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

func (c *Client[I, R]) GetSinkSession(url string) (*core.SinkSession[I, core.BatchState], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session, nil
	} else {
		session = c.sinkSessions.GetOrInsert(
			url,
			func() *core.SinkSession[I, core.BatchState] {
				return c.startSinkSession(url)
			},
		)
		return session, nil
	}
}

func (c *Client[I, R]) SourceSessions() []string {
	return c.sourceSessions.Keys()
}

func (c *Client[I, R]) SourceInfo(url string) (*core.SourceSessionInfo[core.BatchState], error) {
	if session, ok := c.sourceSessions.Get(url); ok {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (c *Client[I, R]) SinkSessions() []string {
	return c.sinkSessions.Keys()
}

func (c *Client[I, R]) SinkInfo(url string) (*core.SinkSessionInfo[core.BatchState], error) {
	if session, ok := c.sinkSessions.Get(url); ok {
		return session.Info(), nil
	} else {
		return nil, ErrInvalidSession
	}
}

func (c *Client[I, R]) Send(url string, id I) error {
	session, err := c.GetSourceSession(url)
	if err != nil {
		return err
	}
	session.Enqueue(id)
	return nil
}

func (c *Client[I, R]) Receive(url string, id I) error {
	session, err := c.GetSinkSession(url)
	if err != nil {
		return err
	}
	return session.Enqueue(id)
}

func (c *Client[I, R]) CloseSource(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CloseSource", "url", url)
	session, err := c.GetSourceSession(url)
	if err != nil {
		log.Debugw("exit", "object", "Client", "method", "CloseSource", "err", err)
		return err
	}
	err = session.Close()
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
	err = session.Close()
	log.Debugw("exit", "object", "Client", "method", "CloseSink", "err", err)
	return err
}

// CancelSource cancels the source session with the given URL.
func (c *Client[I, R]) CancelSource(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CancelSource", "url", url)
	session, err := c.GetSourceSession(url)
	if err != nil {
		log.Debugw("exit", "object", "Client", "method", "CancelSource", "err", err)
		return err
	}
	err = session.Cancel()
	log.Debugw("exit", "object", "Client", "method", "CancelSource", "err", err)
	return err
}

// CancelSink cancels the sink session with the given URL.
func (c *Client[I, R]) CancelSink(url string) error {
	log.Debugw("enter", "object", "Client", "method", "CancelSink", "url", url)
	session, err := c.GetSinkSession(url)
	if err != nil {
		log.Debugw("exit", "object", "Client", "method", "CancelSink", "err", err)
		return err
	}
	err = session.Cancel()
	log.Debugw("exit", "object", "Client", "method", "CancelSink", "err", err)
	return err
}
