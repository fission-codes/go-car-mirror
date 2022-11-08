package carmirror

type BatchBlockReceiver[I BlockId, B Block[I]] interface {
	HandleList(B) error
}

// TODO: How to specify combo of BlockReceiver and Flushable?
type SimpleBatchBlockReceiver[I BlockId, B Block[I], R BlockReceiver[I, B]] struct {
	blockReceiver R
}

func (sbbr *SimpleBatchBlockReceiver[I, B, R]) HandleList(list []B) error {
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
