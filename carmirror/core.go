package carmirror

type Iterator[T any] interface {
	// TODO: Fill in
	Next() (T, bool)
}

// BlockId represents a unique identifier for a Block.
// This interface only represents the identifier, not the Block.
type BlockId interface {
	// TODO: What is needed to ensure BlockId can be used as keys?

	// String returns the BlockId as a string.
	// This is useful when the BlockId must be represented as a string (e.g. when used as a key in a map).
	String() string
}

// Block is an immutable data block referenced by a unique ID.
type Block[I BlockId] interface {
	// Id returns the BlockId for the Block.
	Id() I

	// TODO: iterator over I
	// Children returns a list of `BlockId`s linked to from the Block.
	Children() []I
}

// ReadableBlockStore represents read operations for a store of blocks.
type BlockStore[I BlockId, B Block[I]] interface {
	// Get gets the block from the blockstore with the given ID.
	Get(I) B

	// Add adds the block to the blockstore.
	Add(B)

	// Has returns true if the blockstore has a block with the given ID.
	Has(I) bool

	// All returns a lazy iterator over all block IDs in the blockstore.
	All() Iterator[B]
}

type MutablePointerResolver[I BlockId] interface {
	// Resolve attempts to resolve ptr into a block ID.
	Resolve(ptr string) (id I, err error)
}

// BlockIdFilter is anything similar to a bloom filter that can efficiently (and without perfect accuracy) keep track of a list of `BlockId`s.
type BlockIdFilter[I BlockId] interface {
	// Add adds a BlockId to the Filter.
	Add(id I)

	// Has returns true (sometimes) if Add(BlockId) has been called..
	Has(id I) bool

	// Merge merges two Filters together.
	Merge(other BlockIdFilter[I]) BlockIdFilter[I]
}

type Flushable interface {
	Flush()
}

// BlockSender is responsible for sending blocks - immediately and asynchronously, or via a buffer.
// The details are up to the implementor.
type BlockSender[I BlockId, B Block[I]] interface {
	Flushable

	Send(B)
}

// BlockReceiver is responsible for receiving blocks.
type BlockReceiver[I BlockId, B Block[I]] interface {
	// Receive is called on receipt of a new block.
	Receive(B)
}

// StatusSender is responsible for sending status.
// The key intuition of CAR Mirror is that status can be sent efficiently using a lossy filter.
// The StatusSender will therefore usually batch reported information and send it in bulk to the ReceiverSession.
type StatusSender[F BlockIdFilter[I], I BlockId] interface {
	// TODO: iterator over I
	Send(have F, want []I)
}

// StatusAccumulator is responsible for collecting status.
type StatusAccumulator[F BlockIdFilter[I], I BlockId, S StatusSender[F, I]] interface {
	Have(I)
	Need(I)
	Receive(I)
	Send(S)
}

// StatusReceiver is responsible for receiving a status.
type StatusReceiver[F BlockIdFilter[I], I BlockId] interface {
	// TODO: iterator over I
	HandleStatus(have F, want []I)
}

// Orchestrator is responsible for managing the flow of blocks and/or status.
type Orchestrator interface {
	BeginSend()
	EndSend()
	BeginReceipt()
	EndReceipt()
	BeginFlush()
	EndFlush()
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

func (rs *ReceiverSession[ST, A, S, O, I, B, F]) AccumulateStatus(id I) {
	// rs.orchestrator.BeginSend()
	// block := rs.store.Get(id)
	// // TODO: Equiv of Option in Go.  Return (ok, thing), or (thing, err)?
	// if block != nil {
	// 	rs.accumulator.Have(id)
	// 	for _, child := range block.Children() {
	// 		rs.AccumulateStatus(child)
	// 	}
	// } else {
	// 	rs.accumulator.Need(id)
	// }
	// rs.orchestrator.EndSend()
}
