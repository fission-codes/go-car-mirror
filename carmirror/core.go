package carmirror

import (
	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/iterator"
)

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block.
type BlockId interface {
	comparable
	// TODO: What is needed to ensure BlockId can be used as keys?

	// String returns the BlockId as a string.
	// This is useful when the BlockId must be represented as a string (e.g. when used as a key in a map).
	String() string
}

// Block is an immutable data block referenced by a unique ID.
type Block[I BlockId] interface {
	// TODO: Should I add an iterator type here?

	// Id returns the BlockId for the Block.
	Id() I

	// TODO: iterator over I
	// Links returns a list of `BlockId`s linked to from the Block.
	Links() iterator.Iterator[I]
}

// ReadableBlockStore represents read operations for a store of blocks.
type ReadableBlockStore[I BlockId, B Block[I]] interface {
	// Get gets the block from the blockstore with the given ID.
	Get(I) (B, error)

	// Has returns true if the blockstore has a block with the given ID.
	Has(I) (bool, error)

	// All returns a lazy iterator over all block IDs in the blockstore.
	All() iterator.Iterator[B]

	// TODO: Need a channel version?
	// AllKeysChan() (<-chan I, error)
}

type BlockStore[I BlockId, B Block[I]] interface {
	ReadableBlockStore[I, B]

	// Put puts a given block to the blockstore.
	Put(B) error

	// PutMany puts a slice of blocks at the same time using batching
	// capabilitie of the underlying blockstore if possible.
	PutMany([]B, error)
}

type MutablePointerResolver[I BlockId] interface {
	// Resolve attempts to resolve ptr into a block ID.
	Resolve(ptr string) (I, error)
}

// BlockIdFilter is anything similar to a bloom filter that can efficiently (and without perfect accuracy) keep track of a list of `BlockId`s.
type BlockIdFilter[I BlockId] interface {
	// Add adds a BlockId to the Filter.
	Add(id I) error

	// Has returns true (sometimes) if Add(BlockId) has been called..
	Has(id I) (bool, error)

	// Merge merges two Filters together.
	Merge(other BlockIdFilter[I]) (BlockIdFilter[I], error)
}

type Flushable interface {
	Flush() error
}

// BlockSender is responsible for sending blocks - immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId, B Block[I]] interface {
	Flushable

	Send(B) error
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId, B Block[I]] interface {
	// Receive is called on receipt of a new block.
	Receive(B) error
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[F BlockIdFilter[I], I BlockId] interface {
	// TODO: iterator over I
	Send(have F, want []I) error
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[F BlockIdFilter[I], I BlockId] interface {
	// TODO: iterator over I
	Receive(have F, want []I) error
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[F BlockIdFilter[I], I BlockId, S StatusSender[F, I]] interface {
	Have(I) error
	Want(I) error
	Receive(I) error
	Send(S) error
}

// Orchestrator is responsible for managing the flow of blocks and/or status.
type Orchestrator interface {
	BeginSend() error
	EndSend() error
	BeginReceive() error
	EndReceive() error
	BeginFlush() error
	EndFlush() error
}

type ReceiverSession[
	ST BlockStore[I, B],
	A StatusAccumulator[F, I, S],
	S StatusSender[F, I],
	O Orchestrator,
	I BlockId,
	B Block[I],
	F BlockIdFilter[I],
] struct {
	accumulator  A
	status       S
	orchestrator O
	store        ST
}

func (rs *ReceiverSession[ST, A, S, O, I, B, F]) AccumulateStatus(id I) error {
	if err := rs.orchestrator.BeginSend(); err != nil {
		return err
	}
	defer rs.orchestrator.EndSend()

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

	links := block.Links()
	for {
		link, err := links.Next()

		if err == iterator.Done {
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

func (rs *ReceiverSession[ST, A, S, O, I, B, F]) Flush() error {
	if err := rs.orchestrator.BeginFlush(); err != nil {
		return err
	}

	if err := rs.accumulator.Send(rs.status); err != nil {
		return err
	}

	if err := rs.orchestrator.EndFlush(); err != nil {
		return err
	}

	return nil
}

func (rs *ReceiverSession[ST, A, S, O, I, B, F]) Receive(block B) error {
	if err := rs.orchestrator.BeginReceive(); err != nil {
		return err
	}
	defer rs.orchestrator.EndReceive()

	if err := rs.store.Put(block); err != nil {
		return err
	}

	if err := rs.accumulator.Receive(block.Id()); err != nil {
		return err
	}

	links := block.Links()
	for {
		link, err := links.Next()

		if err == iterator.Done {
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

type SenderSession[
	I BlockId,
	B Block[I],
	ST BlockStore[I, B],
	F BlockIdFilter[I],
	S BlockSender[I, B],
	O Orchestrator,
	K comparable,
] struct {
	store        ST
	blockSender  S
	orchestrator O
	filter       F
	sent         map[I]bool
}

func (ss *SenderSession[I, B, ST, F, S, O, K]) Send(id I) error {
	if err := ss.orchestrator.BeginSend(); err != nil {
		return err
	}
	defer ss.orchestrator.EndSend()

	filterHasId, err := ss.filter.Has(id)
	if err != nil {
		return err
	}
	if !filterHasId {
		ss.sent[id] = true

		block, err := ss.store.Get(id)
		if err != nil && err == errors.BlockNotFound {
			return err
		}

		// We have the block
		if err == nil {
			if err := ss.blockSender.Send(block); err != nil {
				return err
			}

			links := block.Links()
			for {
				link, err := links.Next()

				if err == iterator.Done {
					break
				}

				if err != nil {
					return err
				}

				filterHasId, err := ss.filter.Has(link)
				if err != nil {
					return err
				}
				if !filterHasId {
					if err := ss.Send(link); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (ss *SenderSession[I, B, ST, F, S, O, K]) Flush() error {
	if err := ss.orchestrator.BeginFlush(); err != nil {
		return err
	}
	defer ss.orchestrator.EndFlush()

	if err := ss.blockSender.Flush(); err != nil {
		return err
	}

	return nil
}

func (ss *SenderSession[I, B, ST, F, S, O, K]) Receive(have F, want []I) error {
	if err := ss.orchestrator.BeginReceive(); err != nil {
		return err
	}
	defer ss.orchestrator.EndReceive()

	for _, id := range want {
		if err := ss.Send(id); err != nil {
			return err
		}
	}

	return nil
}
