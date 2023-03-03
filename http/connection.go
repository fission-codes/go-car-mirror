package http

import (
	"bufio"
	"io"
	"net/http"

	"github.com/fission-codes/go-car-mirror/batch"
	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	messages "github.com/fission-codes/go-car-mirror/messages"
	"github.com/fission-codes/go-car-mirror/stats"
)

const CONTENT_TYPE_CBOR = "application/cbor"

type RequestBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	client          *http.Client
	url             string
	responseHandler *batch.SimpleBatchStatusReceiver[I]
}

func (bbs *RequestBatchBlockSender[I, R]) SendList(blocks []core.RawBlock[I]) error {
	log.Debugw("enter", "object", "RequestBatchBlockSender", "method", "SendList", "blocks", len(blocks))
	// We never send block requests for empty batches.
	if len(blocks) == 0 {
		log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "blocks", len(blocks))
		return nil
	}

	message := messages.NewBlocksMessage[I, R](blocks)
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
		log.Errorw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
		// HERE: we're getting a TCP dial error here and returning it.
		return err
	} else {
		if resp.StatusCode == http.StatusAccepted {
			log.Debugw("post response", "object", "RequestBatchBlockSender", "method", "SendList", "url", bbs.url)
			responseMessage := messages.StatusMessage[I, R]{}
			bufferedReader := bufio.NewReader(resp.Body)
			if err := responseMessage.Read(bufferedReader); err != nil {
				log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
				return err
			}
			if err := resp.Body.Close(); err != nil {
				log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList", "error", err)
				return err
			}
			// TODO: Handle error
			bbs.responseHandler.HandleStatus(responseMessage.Have.Any(), responseMessage.Want)
			log.Debugw("exit", "object", "RequestBatchBlockSender", "method", "SendList")
			return nil
		} else {
			// There was no trouble posting.  The server just didn't like the request.
			// Returns 400 bad request after the content has already been transferred successfully in a previous request.
			log.Debugw("Unexpected response", "object", "RequestBatchBlockSender", "status", resp.Status)
			return ErrInvalidResponse
		}
	}
}

func (bbs *RequestBatchBlockSender[I, R]) Close() error {
	return nil
}

type ResponseBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages chan<- *messages.BlocksMessage[I, R]
}

func (bbs *ResponseBatchBlockSender[I, R]) SendList(blocks []core.RawBlock[I]) error {
	log.Debugw("ResponseBatchBlockSender - enter", "method", "SendList", "blocks", len(blocks))
	bbs.messages <- messages.NewBlocksMessage[I, R](blocks)
	log.Debugw("ResponseBatchBlockSender - exit", "method", "SendList")
	return nil
}

func (bbs *ResponseBatchBlockSender[I, R]) Close() error {
	close(bbs.messages)
	return nil
}

type RequestStatusSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	client          *http.Client
	url             string
	responseHandler batch.BatchBlockReceiver[I]
}

func (ss *RequestStatusSender[I, R]) SendStatus(have filter.Filter[I], want []I) error {
	log.Debugw("enter", "object", "RequestStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
	// For requests, we never send status messages if we already have all the blocks we want.
	if len(want) == 0 {
		log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
		return nil
	}

	message := messages.NewStatusMessage[I, R](have, want)
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
			responseMessage := messages.BlocksMessage[I, R]{}
			bufferedReader := bufio.NewReader(resp.Body)
			if err := responseMessage.Read(bufferedReader); err != io.EOF { // read expected to terminate with EOF
				log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
				return err
			}
			if err := resp.Body.Close(); err != nil {
				log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
				return err
			}
			// TODO: We requested the blocks in want.  We got back blocks.
			// Where are we checking if any requested roots were not returned, so we don't ask for them again?
			// Recall that per spec, the requested roots (i.e. want) must always be returned.
			err := ss.responseHandler.HandleList(responseMessage.Car.Blocks)
			log.Debugw("exit", "object", "RequestStatusSender", "method", "SendStatus", "error", err)
			return err
		} else {
			// TODO: Per spec, if the server returns 404, it could find no more new root CIDs.  Handle this case.

			log.Debugw("Unexpected response", "object", "RequestBatchBlockSender", "status", resp.Status)
			return ErrInvalidResponse
		}
	}
}

func (ss *RequestStatusSender[I, R]) Close() error {
	return nil
}

type ResponseStatusSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages chan<- *messages.StatusMessage[I, R]
}

func (ss *ResponseStatusSender[I, R]) SendStatus(have filter.Filter[I], want []I) error {
	log.Debugw("enter", "object", "ResponseStatusSender", "method", "SendStatus", "have", have.Count(), "want", len(want))
	ss.messages <- messages.NewStatusMessage[I, R](have, want)
	log.Debugw("exit", "object", "ResponseStatusSender", "method", "SendStatus")
	return nil
}

func (ss *ResponseStatusSender[I, R]) Close() error {
	close(ss.messages)
	return nil
}

type HttpServerSourceConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	*batch.GenericBatchSourceConnection[I, R]
	messages chan *messages.BlocksMessage[I, R]
}

func NewHttpServerSourceConnection[I core.BlockId, R core.BlockIdRef[I]](stats stats.Stats, instrument instrumented.InstrumentationOptions) *HttpServerSourceConnection[I, R] {
	return &HttpServerSourceConnection[I, R]{
		(*batch.GenericBatchSourceConnection[I, R])(batch.NewGenericBatchSourceConnection[I, R](stats, instrument, false)), // Responder
		make(chan *messages.BlocksMessage[I, R]),
	}
}

func (conn *HttpServerSourceConnection[I, R]) DeferredSender(batchSize uint32) core.BlockSender[I] {
	return conn.Sender(&ResponseBatchBlockSender[I, R]{conn.messages}, batchSize)
}

func (conn *HttpServerSourceConnection[I, R]) DeferredBatchSender() batch.BatchBlockSender[I] {
	return &ResponseBatchBlockSender[I, R]{conn.messages}
}

func (conn *HttpServerSourceConnection[I, R]) PendingResponse() *messages.BlocksMessage[I, R] {
	return <-conn.messages
}

type HttpServerSinkConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	*batch.GenericBatchSinkConnection[I, R]
	messages chan *messages.StatusMessage[I, R]
}

func NewHttpServerSinkConnection[I core.BlockId, R core.BlockIdRef[I]](stats stats.Stats, instrument instrumented.InstrumentationOptions) *HttpServerSinkConnection[I, R] {
	return &HttpServerSinkConnection[I, R]{
		(*batch.GenericBatchSinkConnection[I, R])(batch.NewGenericBatchSinkConnection[I, R](stats, instrument, false)), // Responder
		make(chan *messages.StatusMessage[I, R]),
	}
}

func (conn *HttpServerSinkConnection[I, R]) DeferredSender() core.StatusSender[I] {
	return conn.Sender(&ResponseStatusSender[I, R]{conn.messages})
}

func (conn *HttpServerSinkConnection[I, R]) PendingResponse() *messages.StatusMessage[I, R] {
	return <-conn.messages
}

type HttpClientSourceConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	*batch.GenericBatchSourceConnection[I, R]
	client *http.Client
	url    string
}

func NewHttpClientSourceConnection[I core.BlockId, R core.BlockIdRef[I]](client *http.Client, url string, stats stats.Stats, instrument instrumented.InstrumentationOptions) *HttpClientSourceConnection[I, R] {
	return &HttpClientSourceConnection[I, R]{
		batch.NewGenericBatchSourceConnection[I, R](stats, instrument, true), // Client is requester
		client,
		url,
	}
}

func (conn *HttpClientSourceConnection[I, R]) ImmediateSender(session *core.SourceSession[I, batch.BatchState], batchSize uint32) core.BlockSender[I] {
	return conn.Sender(&RequestBatchBlockSender[I, R]{conn.client, conn.url, conn.Receiver(session)}, batchSize)
}

type HttpClientSinkConnection[I core.BlockId, R core.BlockIdRef[I]] struct {
	*batch.GenericBatchSinkConnection[I, R]
	client *http.Client
	url    string
}

func NewHttpClientSinkConnection[I core.BlockId, R core.BlockIdRef[I]](client *http.Client, url string, stats stats.Stats, instrument instrumented.InstrumentationOptions) *HttpClientSinkConnection[I, R] {
	return &HttpClientSinkConnection[I, R]{
		batch.NewGenericBatchSinkConnection[I, R](stats, instrument, true), // Client is requester
		client,
		url,
	}
}

func (conn *HttpClientSinkConnection[I, R]) ImmediateSender(session *core.SinkSession[I, batch.BatchState]) core.StatusSender[I] {
	return conn.Sender(&RequestStatusSender[I, R]{conn.client, conn.url, conn.Receiver(session)})
}
