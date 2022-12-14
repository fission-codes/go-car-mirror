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
	responseHandler *core.SimpleBatchStatusReceiver[I]
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
			bbs.responseHandler.HandleStatus(responseMessage.State, responseMessage.Have.Any(), responseMessage.Want)
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
