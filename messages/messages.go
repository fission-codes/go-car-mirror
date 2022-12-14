package messages

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/fission-codes/go-car-mirror/carmirror"
	"github.com/fission-codes/go-car-mirror/filter"
	"github.com/fission-codes/go-car-mirror/util"
	"github.com/fxamacker/cbor/v2"
)

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

// ByteAndBlockReader is an io.Reader that also implements io.ByteReader.
type ByteAndBlockReader interface {
	io.Reader
	io.ByteReader
}

func readBufferWithPrefix(reader ByteAndBlockReader) ([]byte, error) {
	if size, err := binary.ReadUvarint(reader); err != nil {
		return nil, err
	} else {
		buf := make([]byte, size)
		_, err = io.ReadFull(reader, buf)
		return buf, err
	}
}

// ArchiveHeaderWireFormat is the wire format for the archive header.
type ArchiveHeaderWireFormat[T carmirror.BlockId] struct {
	Version int `json:"version"`
	Roots   []T `json:"roots"`
}

// ArchiveHeader is the header of content-addressable archive (CAR).
type ArchiveHeader[T carmirror.BlockId] ArchiveHeaderWireFormat[T] // Avoid recursion due to cbor marshalling falling back to using MarshalBinary

// Write writes the archive header to the writer.
func (ah *ArchiveHeader[T]) Write(writer io.Writer) error {
	if buf, err := cbor.Marshal((*ArchiveHeaderWireFormat[T])(ah)); err == nil {
		return writeBufferWithPrefix(writer, buf)
	} else {
		return err
	}
}

// Read reads the archive header from the reader.
func (ah *ArchiveHeader[T]) Read(reader ByteAndBlockReader) error {
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
type BlockWireFormat[T carmirror.BlockId, R carmirror.BlockIdRef[T]] struct {
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
func (b *BlockWireFormat[T, R]) Read(reader ByteAndBlockReader) error {
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
func CastBlockWireFormat[T carmirror.BlockId, R carmirror.BlockIdRef[T]](rawBlock carmirror.RawBlock[T]) *BlockWireFormat[T, R] {
	block, ok := rawBlock.(*BlockWireFormat[T, R])
	if ok {
		return block
	} else {
		id := rawBlock.Id()
		return &BlockWireFormat[T, R]{&id, rawBlock.RawData()}
	}
}

// Archive represents a content-addressable archive (CAR).
type Archive[T carmirror.BlockId, R carmirror.BlockIdRef[T]] struct {
	Header ArchiveHeader[T]        `json:"hdr"`
	Blocks []carmirror.RawBlock[T] `json:"blocks"`
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
func (car *Archive[T, R]) Read(reader ByteAndBlockReader) error {
	var err error
	err = car.Header.Read(reader)
	car.Blocks = make([]carmirror.RawBlock[T], 0)
	for err == nil {
		block := BlockWireFormat[T, R]{}
		if err = block.Read(reader); err == nil {
			car.Blocks = append(car.Blocks, &block)
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
type BlocksMessage[T carmirror.BlockId, R carmirror.BlockIdRef[T], F carmirror.Flags] struct {
	State F
	Car   Archive[T, R]
}

// NewBlocksMessage creates a new blocks message.
func NewBlocksMessage[
	T carmirror.BlockId,
	R carmirror.BlockIdRef[T],
	F carmirror.Flags,
](state F, blocks []carmirror.RawBlock[T]) *BlocksMessage[T, R, F] {
	return &BlocksMessage[T, R, F]{
		state,
		Archive[T, R]{
			ArchiveHeader[T]{
				1,
				util.Map(blocks, func(blk carmirror.RawBlock[T]) T { return blk.Id() }),
			},
			blocks,
		},
	}
}

// Write writes the blocks message to the writer.
func (msg *BlocksMessage[T, B, F]) Write(writer io.Writer) error {
	if data, err := cbor.Marshal(msg.State); err != nil {
		return err
	} else {
		if err = writeBufferWithPrefix(writer, data); err != nil {
			return err
		}
	}

	return msg.Car.Write(writer)
}

// Read reads the blocks message from the reader.
func (msg *BlocksMessage[T, B, F]) Read(reader ByteAndBlockReader) error {
	if data, err := readBufferWithPrefix(reader); err != nil {
		return err
	} else {
		if err = cbor.Unmarshal(data, &msg.State); err != nil {
			return err
		}
	}
	return msg.Car.Read(reader)
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *BlocksMessage[T, B, F]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *BlocksMessage[T, B, F]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

// StatusMessageWireFormat is the wire format of a status message.
type StatusMessageWireFormat[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags] struct {
	State S                           `json:"state"`
	Have  *filter.FilterWireFormat[I] `json:"have"`
	Want  []I                         `json:"want"`
}

// StatusMessage represents a status message.
type StatusMessage[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags] StatusMessageWireFormat[I, R, S]

// NewStatusMessage creates a new status message.
func NewStatusMessage[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags](state S, have filter.Filter[I], want []I) *StatusMessage[I, R, S] {
	return &StatusMessage[I, R, S]{
		state,
		filter.NewFilterWireFormat(have),
		want,
	}
}

// Write writes the status message to the writer.
func (msg *StatusMessage[I, R, S]) Write(writer io.Writer) error {
	if data, err := cbor.Marshal((*StatusMessageWireFormat[I, R, S])(msg)); err != nil {
		return err
	} else {
		return writeBufferWithPrefix(writer, data)
	}
}

// Read reads the status message from the reader.
func (msg *StatusMessage[I, R, S]) Read(reader ByteAndBlockReader) error {
	if data, err := readBufferWithPrefix(reader); err != nil {
		return err
	} else {
		return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R, S])(msg))
	}
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (msg *StatusMessage[I, R, S]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (msg *StatusMessage[I, R, S]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

// MarshalCBOR implements the cbor.Marshaler interface.
func (msg *StatusMessage[I, R, S]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal((*StatusMessageWireFormat[I, R, S])(msg))
}

// UnmarshalCBOR implements the cbor.Unmarshaler interface.
func (msg *StatusMessage[I, R, S]) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R, S])(msg))
}
