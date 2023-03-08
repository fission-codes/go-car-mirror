package messages

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
	"github.com/fxamacker/cbor/v2"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

func writeUvarint(writer io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	sz := binary.PutUvarint(buf, v)
	_, err := writer.Write(buf[0:sz])
	return err
}

func writeBufferWithPrefix(writer io.Writer, buf []byte) error {
	if err := writeUvarint(writer, uint64(len(buf))); err != nil {
		return err
	}
	_, err := writer.Write(buf)
	return err
}

func readBufferWithPrefix(reader core.ByteAndBlockReader) ([]byte, error) {
	if size, err := binary.ReadUvarint(reader); err != nil {
		return nil, err
	} else {
		buf := make([]byte, size)
		_, err = io.ReadFull(reader, buf)
		return buf, err
	}
}

// ArchiveHeaderWireFormat is the wire format for the archive header.
type ArchiveHeaderWireFormat[T core.BlockId] struct {
	Version int `json:"version"`
	Roots   []T `json:"roots"`
}

// ArchiveHeader is the header of content-addressable archive (CAR).
type ArchiveHeader[T core.BlockId] ArchiveHeaderWireFormat[T] // Avoid recursion due to cbor marshalling falling back to using MarshalBinary

// Write writes the archive header to the writer.
func (ah *ArchiveHeader[T]) Write(writer io.Writer) error {
	if buf, err := cbor.Marshal((*ArchiveHeaderWireFormat[T])(ah)); err == nil {
		return writeBufferWithPrefix(writer, buf)
	} else {
		return err
	}
}

// Read reads the archive header from the reader.
func (ah *ArchiveHeader[T]) Read(reader core.ByteAndBlockReader) error {
	if buf, err := readBufferWithPrefix(reader); err == nil {
		return cbor.Unmarshal(buf, (*ArchiveHeaderWireFormat[T])(ah))
	} else {
		return err
	}
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (ah *ArchiveHeader[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (ah *ArchiveHeader[T]) UnmarshalBinary(data []byte) error {
	return ah.Read(bytes.NewBuffer(data))
}

// BlockWireFormat is the wire format for a block id or block id reference.
type BlockWireFormat[T core.BlockId, R core.BlockIdRef[T]] struct {
	IdRef R      `json:"id"`
	Data  []byte `json:"data"`
}

// BlockWireFormat is the wire format.
func (b *BlockWireFormat[T, R]) Write(writer io.Writer) error {
	var (
		err error
		buf []byte
	)

	if buf, err = (*b.IdRef).MarshalBinary(); err != nil {
		return err
	}
	if err = writeUvarint(writer, uint64(len(buf)+len(b.Data))); err != nil {
		return err
	}
	if _, err = writer.Write(buf); err != nil {
		return err
	}
	_, err = writer.Write(b.Data)
	return err
}

// Read reads from the reader into the block wire format.
func (b *BlockWireFormat[T, R]) Read(reader core.ByteAndBlockReader) error {
	var (
		err   error
		size  uint64
		count int
	)
	if size, err = binary.ReadUvarint(reader); err != nil {
		return err
	}
	reader = bufio.NewReader(io.LimitReader(reader, int64(size)))
	if b.IdRef == nil {
		b.IdRef = new(T)
	}
	if count, err = b.IdRef.Read(reader); err != nil {
		log.Debugw("failed to read block id", "error", err)
		return err
	}
	b.Data = make([]byte, int(size)-count)
	_, err = io.ReadFull(reader, b.Data)
	return err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (b *BlockWireFormat[T, R]) UnmarshalBinary(data []byte) error {
	return b.Read(bytes.NewBuffer(data))
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (b *BlockWireFormat[T, R]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := b.Write(&buf)
	return buf.Bytes(), err
}

// Id returns the block id.
func (b *BlockWireFormat[T, R]) Id() T {
	return *b.IdRef
}

// RawData returns the raw data of the encoded block.
func (b *BlockWireFormat[T, R]) RawData() []byte {
	return b.Data
}

// Size returns the size of the block.
func (b *BlockWireFormat[T, R]) Size() int64 {
	return int64(len(b.Data))
}

// CastBlockWireFormat casts a raw block to a block wire format.
func CastBlockWireFormat[T core.BlockId, R core.BlockIdRef[T]](rawBlock core.RawBlock[T]) *BlockWireFormat[T, R] {
	block, ok := rawBlock.(*BlockWireFormat[T, R])
	if ok {
		return block
	} else {
		id := rawBlock.Id()
		return &BlockWireFormat[T, R]{&id, rawBlock.RawData()}
	}
}

// Archive represents a content-addressable archive (CAR).
type Archive[T core.BlockId, R core.BlockIdRef[T]] struct {
	Header ArchiveHeader[T]   `json:"hdr"`
	Blocks []core.RawBlock[T] `json:"blocks"`
}

// Write writes the archive to the writer.
func (car *Archive[T, R]) Write(writer io.Writer) error {
	if err := car.Header.Write(writer); err == nil {
		for _, rawBlock := range car.Blocks {
			block := CastBlockWireFormat[T, R](rawBlock)
			if err = block.Write(writer); err != nil {
				return err
			}
		}
		return nil
	} else {
		return err
	}
}

// Read reads the archive from the reader.
func (car *Archive[T, R]) Read(reader core.ByteAndBlockReader) error {
	var err error
	err = car.Header.Read(reader)
	car.Blocks = make([]core.RawBlock[T], 0)
	for err == nil {
		block := BlockWireFormat[T, R]{}
		if err = block.Read(reader); err == nil {
			car.Blocks = append(car.Blocks, &block)
		} else {
			if err == io.ErrUnexpectedEOF {
				err = io.EOF
			}
		}
	}

	return err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (car *Archive[T, R]) UnmarshalBinary(data []byte) error {
	return car.Read(bytes.NewBuffer(data))
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (ah *Archive[T, R]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

// BlocksMessage state and the archive.
type BlocksMessage[T core.BlockId, R core.BlockIdRef[T]] struct {
	Car Archive[T, R]
}

// NewBlocksMessage creates a new blocks message.
func NewBlocksMessage[
	T core.BlockId,
	R core.BlockIdRef[T],
](blocks []core.RawBlock[T]) *BlocksMessage[T, R] {
	return &BlocksMessage[T, R]{
		Archive[T, R]{
			ArchiveHeader[T]{
				1,
				util.Map(blocks, func(blk core.RawBlock[T]) T { return blk.Id() }),
			},
			blocks,
		},
	}
}

// Write writes the blocks message to the writer.
func (msg *BlocksMessage[T, B]) Write(writer io.Writer) error {
	// if data, err := cbor.Marshal(msg.State); err != nil {
	// 	return err
	// } else {
	// 	if err = writeBufferWithPrefix(writer, data); err != nil {
	// 		return err
	// 	}
	// }

	return msg.Car.Write(writer)
}

// Read reads the blocks message from the reader.
func (msg *BlocksMessage[T, B]) Read(reader core.ByteAndBlockReader) error {
	// if data, err := readBufferWithPrefix(reader); err != nil {
	// 	log.Debugw("failed to read blocks message", "error", err)
	// 	return err
	// } else {
	// 	if err = cbor.Unmarshal(data, &msg.State); err != nil {
	// 		log.Debugw("failed to unmarshal blocks message state", "error", err)
	// 		return err
	// 	}
	// }
	return msg.Car.Read(reader)
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *BlocksMessage[T, B]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *BlocksMessage[T, B]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

// StatusMessageWireFormat is the wire format of a status message.
type StatusMessageWireFormat[I core.BlockId, R core.BlockIdRef[I]] struct {
	Have *filter.FilterWireFormat[I] `json:"have"`
	Want []I                         `json:"want"`
}

// StatusMessage represents a status message.
type StatusMessage[I core.BlockId, R core.BlockIdRef[I]] StatusMessageWireFormat[I, R]

// NewStatusMessage creates a new status message.
func NewStatusMessage[I core.BlockId, R core.BlockIdRef[I]](have filter.Filter[I], want []I) *StatusMessage[I, R] {
	return &StatusMessage[I, R]{
		filter.NewFilterWireFormat(have),
		want,
	}
}

// Write writes the status message to the writer.
func (msg *StatusMessage[I, R]) Write(writer io.Writer) error {
	if data, err := cbor.Marshal((*StatusMessageWireFormat[I, R])(msg)); err != nil {
		return err
	} else {
		return writeBufferWithPrefix(writer, data)
	}
}

// Read reads the status message from the reader.
func (msg *StatusMessage[I, R]) Read(reader core.ByteAndBlockReader) error {
	if data, err := readBufferWithPrefix(reader); err != nil {
		return err
	} else {
		return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R])(msg))
	}
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (msg *StatusMessage[I, R]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *StatusMessage[I, R]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

// MarshalCBOR implements the cbor.Marshaler interface.
func (msg *StatusMessage[I, R]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal((*StatusMessageWireFormat[I, R])(msg))
}

// UnmarshalCBOR implements the cbor.Unmarshaler interface.
func (msg *StatusMessage[I, R]) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R])(msg))
}
