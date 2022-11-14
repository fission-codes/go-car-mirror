package carmirror

import (
	"github.com/fission-codes/go-car-mirror/errors"
	"github.com/fission-codes/go-car-mirror/iterator"
)

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block.
type BlockId interface {
	// TODO: Can't implement comparable.  See https://github.com/golang/go/issues/56548#issuecomment-1304359937.
	// comparable

	// TODO: What is needed to ensure BlockId can be used as keys or added to a Filter?  Currently just using String.
	Bytes() []byte

	// String returns the BlockId as a string.
	// Currently this is what we use for hash map keys.
	String() string
}

// Block is an immutable data block referenced by a unique ID.
type Block[I BlockId, IT iterator.Iterator[I]] interface {
	// TODO: Should I add an iterator type here?

	// Id returns the BlockId for the Block.
	Id() I

	// Children returns a list of `BlockId`s linked to from the Block.
	Children() IT
}

// This was needed in part to deal with lack of ability to implement comparable in Go 1.18.
type BlockIdHashMap[I BlockId, IT iterator.Iterator[I]] interface {
	Add(I) error
	Has(I) (bool, error)
	Keys() (*IT, error)
}

// ReadableBlockStore represents read operations for a store of blocks.
type ReadableBlockStore[I BlockId, ITI iterator.Iterator[I], ITB iterator.Iterator[B], B Block[I, ITI]] interface {
	// Get gets the block from the blockstore with the given ID.
	Get(I) (B, error)

	// Has returns true if the blockstore has a block with the given ID.
	Has(I) (bool, error)

	// All returns a lazy iterator over all block IDs in the blockstore.
	All() ITB

	// TODO: Need a channel version?
	// AllKeysChan() (<-chan I, error)
}

type BlockStore[I BlockId, ITI iterator.Iterator[I], ITB iterator.Iterator[B], B Block[I, ITI]] interface {
	ReadableBlockStore[I, ITI, ITB, B]

	// Add puts a given block to the blockstore.
	Add(B) error

	// PutMany puts a slice of blocks at the same time using batching
	// capabilitie of the underlying blockstore if possible.
	// PutMany([]B, error)
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
	Merge(other BlockIdFilter[I]) error
}

type Flushable interface {
	Flush() error
}

// BlockSender is responsible for sending blocks - immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId, ITI iterator.Iterator[I], B Block[I, ITI]] interface {
	Flushable

	Send(B) error
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId, ITI iterator.Iterator[I], B Block[I, ITI]] interface {
	Flushable

	// Receive is called on receipt of a new block.
	Receive(B) error
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[F BlockIdFilter[I], I BlockId] interface {
	Send(have F, want []I) error
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[F BlockIdFilter[I], I BlockId] interface {
	Receive(have F, want []I) error
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[F BlockIdFilter[I], I BlockId, S StatusSender[F, I]] interface {
	Have(I) error
	Want(I) error
	Send(S) error
	Receive(I) error
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
	ST BlockStore[I, ITI, ITB, B],
	A StatusAccumulator[F, I, S],
	S StatusSender[F, I],
	O Orchestrator,
	I BlockId,
	B Block[I, ITI],
	ITI iterator.Iterator[I],
	ITB iterator.Iterator[B],
	F BlockIdFilter[I],
] struct {
	accumulator  A
	status       S
	orchestrator O
	store        ST
}

func (rs *ReceiverSession[ST, A, S, O, I, B, ITI, ITB, F]) AccumulateStatus(id I) error {
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

	links := block.Children()
	for {
		link, err := links.Next()

		if err == iterator.ErrDone {
			break
		}

		if err != nil {
			return err
		}

		if err := rs.AccumulateStatus(*link); err != nil {
			return err
		}
	}

	return nil
}

func (rs *ReceiverSession[ST, A, S, O, I, B, ITI, ITB, F]) Flush() error {
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

func (rs *ReceiverSession[ST, A, S, O, I, B, ITI, ITB, F]) Receive(block B) error {
	if err := rs.orchestrator.BeginReceive(); err != nil {
		return err
	}
	defer rs.orchestrator.EndReceive()

	if err := rs.store.Add(block); err != nil {
		return err
	}

	if err := rs.accumulator.Receive(block.Id()); err != nil {
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

		if err := rs.AccumulateStatus(*link); err != nil {
			return err
		}
	}

	return nil
}

type SenderSession[
	I BlockId,
	B Block[I, ITI],
	ST BlockStore[I, ITI, ITB, B],
	F BlockIdFilter[I],
	S BlockSender[I, ITI, B],
	O Orchestrator,
	K comparable,
	ITI iterator.Iterator[I],
	ITB iterator.Iterator[B],
	H BlockIdHashMap[I, ITI],
] struct {
	store        ST
	blockSender  S
	orchestrator O
	filter       F
	sent         H
}

func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Send(id I) error {
	if err := ss.orchestrator.BeginSend(); err != nil {
		return err
	}
	defer ss.orchestrator.EndSend()

	filterHasId, err := ss.filter.Has(id)
	if err != nil {
		return err
	}
	if !filterHasId {
		if err := ss.sent.Add(id); err != nil {
			return err
		}

		block, err := ss.store.Get(id)
		if err != nil && err == errors.BlockNotFound {
			return err
		}

		if err == nil {
			// We have the block

			if err := ss.blockSender.Send(block); err != nil {
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

				filterHasId, err := ss.filter.Has(*link)
				if err != nil {
					return err
				}
				if !filterHasId {
					if err := ss.Send(*link); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Flush() error {
	if err := ss.orchestrator.BeginFlush(); err != nil {
		return err
	}
	defer ss.orchestrator.EndFlush()

	if err := ss.blockSender.Flush(); err != nil {
		return err
	}

	return nil
}

func (ss *SenderSession[I, B, ST, F, S, O, K, ITI, ITB, H]) Receive(have F, want []I) error {
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
