package http

import (
	core "github.com/fission-codes/go-car-mirror/carmirror"
	messages "github.com/fission-codes/go-car-mirror/messages"
)

type ResponseBatchBlockSender[I core.BlockId, R core.BlockIdRef[I]] struct {
	messages chan<- *messages.BlocksMessage[I, R, core.BatchState]
}

func (bbs *ResponseBatchBlockSender[I, R]) SendList(state core.BatchState, blocks []core.RawBlock[I]) error {
	bbs.messages <- messages.NewBlocksMessage[I, R, core.BatchState](state, blocks)
	return nil
}

func (bbs *ResponseBatchBlockSender[I, R]) Close() error {
	close(bbs.messages)
	return nil
}

type ServerSenderConnection[
	I core.BlockId,
	R core.BlockIdRef[I],
] struct {
	messages     chan *messages.BlocksMessage[I, R, core.BatchState]
	maxBatchSize uint32
}

func NewServerSenderConnection[I core.BlockId, R core.BlockIdRef[I]](maxBatchSize uint32) *ServerSenderConnection[I, R] {
	return &ServerSenderConnection[I, R]{
		make(chan *messages.BlocksMessage[I, R, core.BatchState]),
		maxBatchSize,
	}
}

func (conn *ServerSenderConnection[I, R]) ResponseChannel() <-chan *messages.BlocksMessage[I, R, core.BatchState] {
	return conn.messages
}

// OpenBlockSender opens a block sender
// we are on the server side here, so the message will actually be sent in response to a status message
func (conn *ServerSenderConnection[I, R]) OpenBlockSender(orchestrator core.Orchestrator[core.BatchState]) core.BlockSender[I] {
	return core.NewSimpleBatchBlockSender[I](
		&ResponseBatchBlockSender[I, R]{conn.messages},
		orchestrator,
		conn.maxBatchSize,
	)
}
