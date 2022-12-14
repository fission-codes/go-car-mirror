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

type ArchiveHeaderWireFormat[T carmirror.BlockId] struct {
	Version int `json:"version"`
	Roots   []T `json:"roots"`
}

type ArchiveHeader[T carmirror.BlockId] ArchiveHeaderWireFormat[T] // Avoid recursion due to cbor marshalling falling back to using MarshalBinary

func (ah *ArchiveHeader[T]) Write(writer io.Writer) error {
	if buf, err := cbor.Marshal((*ArchiveHeaderWireFormat[T])(ah)); err == nil {
		return writeBufferWithPrefix(writer, buf)
	} else {
		return err
	}
}

func (ah *ArchiveHeader[T]) Read(reader ByteAndBlockReader) error {
	if buf, err := readBufferWithPrefix(reader); err == nil {
		return cbor.Unmarshal(buf, (*ArchiveHeaderWireFormat[T])(ah))
	} else {
		return err
	}
}

func (ah *ArchiveHeader[T]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

func (ah *ArchiveHeader[T]) UnmarshalBinary(data []byte) error {
	return ah.Read(bytes.NewBuffer(data))
}

type BlockWireFormat[T carmirror.BlockId, R carmirror.BlockIdRef[T]] struct {
	IdRef R      `json:"id"`
	Data  []byte `json:"data"`
}

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

func (b *BlockWireFormat[T, R]) UnmarshalBinary(data []byte) error {
	return b.Read(bytes.NewBuffer(data))
}

func (b *BlockWireFormat[T, R]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := b.Write(&buf)
	return buf.Bytes(), err
}

func (b *BlockWireFormat[T, R]) Id() T {
	return *b.IdRef
}

func (b *BlockWireFormat[T, R]) RawData() []byte {
	return b.Data
}

func (b *BlockWireFormat[T, R]) Size() int64 {
	return int64(len(b.Data))
}

func CastBlockWireFormat[T carmirror.BlockId, R carmirror.BlockIdRef[T]](rawBlock carmirror.RawBlock[T]) *BlockWireFormat[T, R] {
	block, ok := rawBlock.(*BlockWireFormat[T, R])
	if ok {
		return block
	} else {
		id := rawBlock.Id()
		return &BlockWireFormat[T, R]{&id, rawBlock.RawData()}
	}
}

type Archive[T carmirror.BlockId, R carmirror.BlockIdRef[T]] struct {
	Header ArchiveHeader[T]        `json:"hdr"`
	Blocks []carmirror.RawBlock[T] `json:"blocks"`
}

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

func (car *Archive[T, R]) UnmarshalBinary(data []byte) error {
	return car.Read(bytes.NewBuffer(data))
}

func (ah *Archive[T, R]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

type BlocksMessage[T carmirror.BlockId, R carmirror.BlockIdRef[T], F carmirror.Flags] struct {
	State F
	Car   Archive[T, R]
}

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

func (msg *BlocksMessage[T, B, F]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

func (msg *BlocksMessage[T, B, F]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

type StatusMessageWireFormat[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags] struct {
	State S                           `json:"state"`
	Have  *filter.FilterWireFormat[I] `json:"have"`
	Want  []I                         `json:"want"`
}

type StatusMessage[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags] StatusMessageWireFormat[I, R, S]

func NewStatusMessage[I carmirror.BlockId, R carmirror.BlockIdRef[I], S carmirror.Flags](state S, have filter.Filter[I], want []I) *StatusMessage[I, R, S] {
	return &StatusMessage[I, R, S]{
		state,
		filter.NewFilterWireFormat(have),
		want,
	}
}

func (msg *StatusMessage[I, R, S]) Write(writer io.Writer) error {
	if data, err := cbor.Marshal((*StatusMessageWireFormat[I, R, S])(msg)); err != nil {
		return err
	} else {
		return writeBufferWithPrefix(writer, data)
	}
}

func (msg *StatusMessage[I, R, S]) Read(reader ByteAndBlockReader) error {
	if data, err := readBufferWithPrefix(reader); err != nil {
		return err
	} else {
		return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R, S])(msg))
	}
}

func (msg *StatusMessage[I, R, S]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := msg.Write(&buf)
	return buf.Bytes(), err
}

func (msg *StatusMessage[I, R, S]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

func (msg *StatusMessage[I, R, S]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal((*StatusMessageWireFormat[I, R, S])(msg))
}

func (msg *StatusMessage[I, R, S]) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, (*StatusMessageWireFormat[I, R, S])(msg))
}
