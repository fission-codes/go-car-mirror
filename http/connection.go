package http

import (
	"bufio"
	"io"
	"net/http"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	messages "github.com/fission-codes/go-car-mirror/messages"
)

const CONTENT_TYPE_CBOR = "application/cbor"

type RequestBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	client          *http.Client
	url             string
	responseHandler core.StatusReceiver[I, core.BatchState]
}

func (bbs *RequestBatchBlockSender[I, R]) SendList(state core.BatchState, blocks []core.RawBlock[I]) error {
	log.Debugw("enter", "object", "RequestBatchBlockSender", "method", "SendList", "state", state, "blocks", len(blocks))
	message := messages.NewBlocksMessage[I, R](state, blocks)
	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()
		if err := message.Write(writer); err != nil {
			log.Debugw("write error", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
		} else {
			log.Debugw("finished writing batch to request", "object", "RequestBatchBlockSender", "method", "SendList")
		}
	}()
	log.Debugw("post", "object", "RequestBatchBlockSender", "method", "SendList", "url", bbs.url)
	if resp, err := bbs.client.Post(bbs.url, CONTENT_TYPE_CBOR, reader); err != nil {
		log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
		return err
	} else {
		if resp.StatusCode == http.StatusAccepted {
			log.Debugw("post response", "object", "RequestBatchBlockSender", "method", "SendList", "url", bbs.url)
			responseMessage := messages.StatusMessage[I, R, core.BatchState]{}
			bufferedReader := bufio.NewReader(resp.Body)
			if err := responseMessage.Read(bufferedReader); err != nil {
				log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
				return err
			}
			if err := resp.Body.Close(); err != nil {
				log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
				return err
			}
			bbs.responseHandler.HandleStatus(responseMessage.Have.Any(), responseMessage.Want)
			bbs.responseHandler.HandleState(responseMessage.State)
			log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList")
			return nil
		} else {
			log.Debugw("Unexpected response", "object", "RequestBatchBlockSender", "status", resp.Status)
			return ErrInvalidResponse
		}
	}
}

func (bbs *RequestBatchBlockSender[I, R]) Close() error {
	return nil
}

type ResponseBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages chan<- *messages.BlocksMessage[I, R, core.BatchState]
}

func (bbs *ResponseBatchBlockSender[I, R]) SendList(state core.BatchState, blocks []core.RawBlock[I]) error {
	log.Debugw("ResponseBatchBlockSender - enter", "method", "SendList", "state", state, "blocks", len(blocks))
	bbs.messages <- messages.NewBlocksMessage[I, R](state, blocks)
	log.Debugw("ResponseBatchBlockSender - exit", "method", "SendList")
	return nil
}

func (bbs *ResponseBatchBlockSender[I, R]) Close() error {
	//close(bbs.messages)
	return nil
}

type RequestStatusSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	orchestrator    core.Orchestrator[core.BatchState]
	client          *http.Client
	url             string
	responseHandler core.BatchBlockReceiver[I]
}

func (ss *RequestStatusSender[I, R]) SendStatus(have filter.Filter[I], want []I) error {
	log.Debugw("enter", "object", "RequestStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
	state := ss.orchestrator.State()
	message := messages.NewStatusMessage[I, R](state, have, want)
	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()
		if err := message.Write(writer); err != nil {
			log.Debugw("write error", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
		} else {
			log.Debugw("finished writing batch to request", "object", "RequestStatusSender", "method", "SendStatus")
		}
	}()
	log.Debugw("post", "object", "RequestStatusSender", "method", "SendStatus", "url", ss.url)
	if resp, err := ss.client.Post(ss.url, CONTENT_TYPE_CBOR, reader); err != nil {
		log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
		return err
	} else {
		if resp.StatusCode == http.StatusAccepted {
			log.Debugw("post response", "object", "RequestStatusSender", "method", "SendStatus", "url", ss.url)
			responseMessage := messages.BlocksMessage[I, R, core.BatchState]{}
			bufferedReader := bufio.NewReader(resp.Body)
			if err := responseMessage.Read(bufferedReader); err != io.EOF { // read expected to terminate with EOF
				log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
				return err
			}
			if err := resp.Body.Close(); err != nil {
				log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
				return err
			}
			err := ss.responseHandler.HandleList(responseMessage.State, responseMessage.Car.Blocks)
			log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
			return err
		} else {
			log.Debugw("Unexpected response", "object", "RequestBatchBlockSender", "status", resp.Status)
			return ErrInvalidResponse
		}
	}
}

func (ss *RequestStatusSender[I, R]) Close() error {
	return nil
}

type ResponseStatusSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	orchestrator core.Orchestrator[core.BatchState]
	messages     chan<- *messages.StatusMessage[I, R, core.BatchState]
}

func (ss *ResponseStatusSender[I, R]) SendStatus(have filter.Filter[I], want []I) error {
	log.Debugw("enter", "object", "ResponseStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
	state := ss.orchestrator.State()
	log.Debugw("returning message", "object", "ResponseStatusSender", "method", "SendStatus", "state", state)
	ss.messages <- messages.NewStatusMessage[I, R](state, have, want)
	log.Debugw("exit", "object", "ResponseStatusSender", "method", "SendStatus")
	return nil
}

func (ss *ResponseStatusSender[I, R]) Close() error {
	//close(ss.messages)
	return nil
}

type ClientSourceConnection[
	I core.BlockId,
	R core.BlockIdRef[I],
] struct {
	responseHandler core.StatusReceiver[I, core.BatchState]
	maxBatchSize    uint32
	client          *http.Client
	url             string
}

func NewClientSourceConnection[I core.BlockId, R core.BlockIdRef[I]](
	maxBatchSize uint32,
	client *http.Client,
	url string,
	responseHandler core.StatusReceiver[I, core.BatchState],
) *ClientSourceConnection[I, R] {
	return &ClientSourceConnection[I, R]{
		responseHandler,
		maxBatchSize,
		client,
		url,
	}
}

// OpenBlockSender opens a block sender
// we are on the server side here, so the message will actually be sent in response to a status message
func (conn *ClientSourceConnection[I, R]) OpenBlockSender(orchestrator core.Orchestrator[core.BatchState]) core.BlockSender[I] {
	return core.NewSimpleBatchBlockSender[I](
		&RequestBatchBlockSender[I, R]{conn.client, conn.url, conn.responseHandler},
		orchestrator,
		conn.maxBatchSize,
	)
}

type ServerSourceConnection[
	I core.BlockId,
	R core.BlockIdRef[I],
] struct {
	messages     chan *messages.BlocksMessage[I, R, core.BatchState]
	maxBatchSize uint32
}

func NewServerSourceConnection[I core.BlockId, R core.BlockIdRef[I]](maxBatchSize uint32) *ServerSourceConnection[I, R] {
	return &ServerSourceConnection[I, R]{
		make(chan *messages.BlocksMessage[I, R, core.BatchState]),
		maxBatchSize,
	}
}

func (conn *ServerSourceConnection[I, R]) ResponseChannel() <-chan *messages.BlocksMessage[I, R, core.BatchState] {
	return conn.messages
}

// OpenBlockSender opens a block sender
// we are on the server side here, so the message will actually be sent in response to a status message
func (conn *ServerSourceConnection[I, R]) OpenBlockSender(orchestrator core.Orchestrator[core.BatchState]) core.BlockSender[I] {
	return core.NewSimpleBatchBlockSender[I](
		&ResponseBatchBlockSender[I, R]{conn.messages},
		orchestrator,
		conn.maxBatchSize,
	)
}

type ClientSinkConnection[
	I core.BlockId,
	R core.BlockIdRef[I],
] struct {
	responseHandler core.BatchBlockReceiver[I]
	client          *http.Client
	url             string
}

func NewClientSinkConnection[I core.BlockId, R core.BlockIdRef[I]](
	client *http.Client,
	url string,
	responseHandler core.BatchBlockReceiver[I],
) *ClientSinkConnection[I, R] {
	return &ClientSinkConnection[I, R]{
		responseHandler,
		client,
		url,
	}
}

// OpenStatusSender opens a client-side status sender
func (conn *ClientSinkConnection[I, R]) OpenStatusSender(orchestrator core.Orchestrator[core.BatchState]) core.StatusSender[I] {
	return &RequestStatusSender[I, R]{
		orchestrator,
		conn.client,
		conn.url,
		conn.responseHandler,
	}
}

type ServerSinkConnection[
	I core.BlockId,
	R core.BlockIdRef[I],
] struct {
	messages     chan *messages.StatusMessage[I, R, core.BatchState]
	maxBatchSize uint32
}

func NewServerSinkConnection[I core.BlockId, R core.BlockIdRef[I]](maxBatchSize uint32) *ServerSinkConnection[I, R] {
	return &ServerSinkConnection[I, R]{
		make(chan *messages.StatusMessage[I, R, core.BatchState]),
		maxBatchSize,
	}
}

func (conn *ServerSinkConnection[I, R]) ResponseChannel() <-chan *messages.StatusMessage[I, R, core.BatchState] {
	return conn.messages
}

// OpenStatusSender opens a status sender
// we are on the server side here, so the message will actually be sent in response to a blocks message
func (conn *ServerSinkConnection[I, R]) OpenStatusSender(orchestrator core.Orchestrator[core.BatchState]) core.StatusSender[I] {
	return &ResponseStatusSender[I, R]{
		orchestrator,
		conn.messages,
	}
}
