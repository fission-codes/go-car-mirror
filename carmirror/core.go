package carmirror

import (
	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/iterator"
)

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block.
type BlockId comparable

// Block is an immutable data block referenced by a unique ID.
type Block[I BlockId] interface {
	// TODO: Should I add an iterator type here?

	// Id returns the BlockId for the Block.
	Id() *I

	// Children returns a list of `BlockId`s linked to from the Block.
	Children() iterator.Iterator[I]
}

// ReadableBlockStore represents read operations for a store of blocks.
type ReadableBlockStore[I BlockId] interface {
	// Get gets the block from the blockstore with the given ID.
	Get(*I) (Block[I], error)

	// Has returns true if the blockstore has a block with the given ID.
	Has(*I) (bool, error)

	// All returns a channel that will receive all of the block IDs in this store.
	All() (<-chan I, error)
}

type BlockStore[I BlockId] interface {
	ReadableBlockStore[I]

	// Add puts a given block to the blockstore.
	Add(Block[I]) error
}

type MutablePointerResolver[I BlockId] interface {
	// Resolve attempts to resolve ptr into a block ID.
	Resolve(ptr string) (I, error)
}

// Filter is anything similar to a bloom filter that can efficiently (and without perfect accuracy) keep track of a list of `BlockId`s.
type Filter[K comparable] interface {
	// DoesNotContain returns true if id is not present in the filter.
	DoesNotContain(id K) bool

	// Merge merges two Filters together.
	Merge(other Filter[K]) (Filter[K], error)
}

// BlockSender is responsible for sending blocks - immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId] interface {
	Send(Block[I]) error
	Flush() error
	Close() error
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId] interface {
	// HandleBlock is called on receipt of a new block.
	HandleBlock(Block[I]) error
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[I BlockId] interface {
	Send(have Filter[I], want []I) error
	Close() error
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[I BlockId] interface {
	HandleStatus(have Filter[I], want []I) error
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[I BlockId] interface {
	Have(*I) error
	Want(*I) error
	Send(StatusSender[I]) error
	Receive(*I) error
}

type SessionEvent uint16

const (
	BEGIN_SESSION SessionEvent = iota
	END_SESSION
	BEGIN_DRAINING
	END_DRAINING
	BEGIN_CLOSE
	END_CLOSE
	BEGIN_FLUSH
	END_FLUSH
	BEGIN_SEND
	END_SEND
	BEGIN_RECEIVE
	END_RECEIVE
	BEGIN_CHECK
	END_CHECK
)

// Internal state...
type Flags uint16

// Orchestrator is responsible for managing the flow of blocks and/or status.
type Orchestrator[F Flags] interface {
	Notify(SessionEvent) error

	// GetState is used to obtain state to send to a remote session.
	GetState() (F, error)

	// ReceiveState is used to receive state from a remote session.
	ReceiveState(F) error

	IsClosed() (bool, error)
}

type SenderConnection[
	F Flags,
	I BlockId,
] interface {
	GetBlockSender(Orchestrator[F]) BlockSender[I]
}

type ReceiverConnection[
	F Flags,
	I BlockId,
] interface {
	GetStatusSender(Orchestrator[F]) StatusSender[I]
}

type ReceiverSession[
	I BlockId,
	F Flags,
] struct {
	accumulator  StatusAccumulator[I]
	connection   ReceiverConnection[F, I]
	orchestrator Orchestrator[F]
	store        BlockStore[I]
	pending      chan Block[I]
	// pending      map[I]bool
	// pendingMutex sync.RWMutex
}

func (rs *ReceiverSession[I, F]) AccumulateStatus(id *I) error {
	// Get block and handle errors
	block, err := rs.store.Get(id)
	if err == errors.BlockNotFound {
		if err := rs.accumulator.Want(id); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	if err := rs.accumulator.Have(id); err != nil {
		return err
	}

	links := block.Children()
	for {
		link, err := links.Next()

		if err == iterator.ErrDone {
			break
		}

		if err != nil {
			return err
		}

		if err := rs.AccumulateStatus(link); err != nil {
			return err
		}
	}

	return nil
}

func (rs *ReceiverSession[I, F]) HandleBlock(block Block[I]) error {
	rs.orchestrator.Notify(BEGIN_RECEIVE)
	defer rs.orchestrator.Notify(END_RECEIVE)

	if err := rs.store.Add(block); err != nil {
		return err
	}

	if err := rs.accumulator.Receive(block.Id()); err != nil {
		return err
	}

	rs.pending <- block
	// rs.pendingMutex.Lock()
	// defer rs.pendingMutex.Unlock()
	// rs.pending[*block.Id()] = true

	return nil
}

func (rs *ReceiverSession[I, F]) Run() error {
	sender := rs.connection.GetStatusSender(rs.orchestrator)

	rs.orchestrator.Notify(BEGIN_SESSION)
	defer func() {
		rs.orchestrator.Notify(END_SESSION)
		sender.Close()
	}()

	isClosed, err := rs.orchestrator.IsClosed()
	if err != nil {
		return err
	}

	for !isClosed {
		// TODO: Look into use of defer for this.
		rs.orchestrator.Notify(BEGIN_CHECK)

		// TODO: Any concerns with hangs here if nothing in pending?  Need goroutines?
		if len(rs.pending) > 0 {
			block := <-rs.pending

			links := block.Children()
			for {
				link, err := links.Next()

				if err == iterator.ErrDone {
					break
				}

				if err != nil {
					return err
				}

				if err := rs.AccumulateStatus(link); err != nil {
					return err
				}
			}
		} else {
			// If we get here it means we have no more blocks to process. In a batch based process that's
			// the only time we'd want to send. But for streaming maybe this should be in a separate loop
			// so it can be triggered by the orchestrator - otherwise we wind up sending a status update every
			// time the pending list becomes empty.

			rs.orchestrator.Notify(BEGIN_SEND)
			rs.accumulator.Send(sender)
			rs.orchestrator.Notify(END_SEND)

		}
		rs.orchestrator.Notify(END_CHECK)
	}
	sender.Close()

	return nil
}

// func (rs *ReceiverSession[I, F, E]) Flush() error {
// 	if err := rs.orchestrator.BeginFlush(); err != nil {
// 		return err
// 	}

// 	if err := rs.accumulator.Send(rs.status); err != nil {
// 		return err
// 	}

// 	if err := rs.orchestrator.EndFlush(); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (rs *ReceiverSession[I, F, E]) Receive(block B) error {
// 	if err := rs.orchestrator.BeginReceive(); err != nil {
// 		return err
// 	}
// 	defer rs.orchestrator.EndReceive()

// 	if err := rs.store.Add(block); err != nil {
// 		return err
// 	}

// 	if err := rs.accumulator.Receive(block.Id()); err != nil {
// 		return err
// 	}

// 	links := block.Children()
// 	for {
// 		link, err := links.Next()

// 		if err == iterator.ErrDone {
// 			break
// 		}

// 		if err != nil {
// 			return err
// 		}

// 		if err := rs.AccumulateStatus(*link); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// type SenderSession[
// 	I BlockId,
// 	B Block[I, ITI],
// 	ST BlockStore[I, ITI, ITB, B],
// 	F Filter[I],
// 	S BlockSender[I, ITI, B],
// 	O Orchestrator,
// 	K comparable,
// 	ITI iterator.Iterator[I],
// 	ITB iterator.Iterator[B],
// ] struct {
// 	store        ST
// 	blockSender  S
// 	orchestrator O
// 	filter       F
// 	sent         H
// }

// func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Send(id I) error {
// 	if err := ss.orchestrator.BeginSend(); err != nil {
// 		return err
// 	}
// 	defer ss.orchestrator.EndSend()

// 	filterHasId, err := ss.filter.Has(id)
// 	if err != nil {
// 		return err
// 	}
// 	if !filterHasId {
// 		if err := ss.sent.Add(id); err != nil {
// 			return err
// 		}

// 		block, err := ss.store.Get(id)
// 		if err != nil && err == errors.BlockNotFound {
// 			return err
// 		}

// 		if err == nil {
// 			// We have the block

// 			if err := ss.blockSender.Send(block); err != nil {
// 				return err
// 			}

// 			links := block.Children()
// 			for {
// 				link, err := links.Next()

// 				if err == iterator.ErrDone {
// 					break
// 				}

// 				if err != nil {
// 					return err
// 				}

// 				filterHasId, err := ss.filter.Has(*link)
// 				if err != nil {
// 					return err
// 				}
// 				if !filterHasId {
// 					if err := ss.Send(*link); err != nil {
// 						return err
// 					}
// 				}
// 			}
// 		}
// 	}

// 	return nil
// }

// func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Flush() error {
// 	if err := ss.orchestrator.BeginFlush(); err != nil {
// 		return err
// 	}
// 	defer ss.orchestrator.EndFlush()

// 	if err := ss.blockSender.Flush(); err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Receive(have F, want []I) error {
// 	if err := ss.orchestrator.BeginReceive(); err != nil {
// 		return err
// 	}
// 	defer ss.orchestrator.EndReceive()

// 	for _, id := range want {
// 		if err := ss.Send(id); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }
