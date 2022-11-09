package carmirror

import "github.com/fission-codes/go-car-mirror/iterator"

type BatchBlockReceiver[I BlockId, ITI iterator.Iterator[I], B Block[I, ITI]] interface {
	HandleList(B) error
}

// TODO: How to specify combo of BlockReceiver and Flushable?
type SimpleBatchBlockReceiver[I BlockId, ITI iterator.Iterator[I], B Block[I, ITI], R BlockReceiver[I, ITI, B]] struct {
	blockReceiver R
}

func (sbbr *SimpleBatchBlockReceiver[I, ITI, B, R]) HandleList(list []B) error {
	for _, block := range list {
		if err := sbbr.blockReceiver.Receive(block); err != nil {
			return err
		}
	}
	if err := sbbr.blockReceiver.Flush(); err != nil {
		return err
	}

	return nil
}
