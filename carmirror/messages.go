package carmirror

import (
	"bufio"
	"bytes"
	"encoding"
	"encoding/binary"
	"io"

	"github.com/fxamacker/cbor/v2"
)

func writeUvarint(writer io.Writer, v uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	sz := binary.PutUvarint(buf, v)
	_, err := writer.Write(buf[0:sz])
	return err
}

func writeBinary[T encoding.BinaryMarshaler](writer io.Writer, v T) (int, error) {
	if bytes, err := v.MarshalBinary(); err != nil {
		return len(bytes), err
	} else {
		wrtn, err := writer.Write(bytes)
		return wrtn, err
	}
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

type ArchiveHeaderWireFormat[T BlockId] struct {
	Version int
	Roots   []T
}

type ArchiveHeader[T BlockId] ArchiveHeaderWireFormat[T] // Avoid recursion due to cbor marshalling falling back to using MarshalBinary

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

type BlockWireFormat[T BlockId, R BlockIdRef[T]] struct {
	Id   R      `json:"id"`
	Data []byte `json:"data"`
}

func (b *BlockWireFormat[T, R]) Write(writer io.Writer) error {
	var (
		err error
		buf []byte
	)

	if buf, err = (*b.Id).MarshalBinary(); err != nil {
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
	if b.Id == nil {
		b.Id = new(T)
	}
	if count, err = b.Id.Read(reader); err != nil {
		return err
	}
	b.Data = make([]byte, int(size)-count)
	_, err = reader.Read(b.Data)
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

type Archive[T BlockId, R BlockIdRef[T]] struct {
	Header ArchiveHeader[T]        `json:"hdr"`
	Blocks []BlockWireFormat[T, R] `json:"blocks"`
}

func (car *Archive[T, R]) Write(writer io.Writer) error {
	if err := car.Header.Write(writer); err == nil {
		for _, block := range car.Blocks {
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
	car.Blocks = car.Blocks[:0]
	for err == nil {
		block := BlockWireFormat[T, R]{}
		if err = block.Read(reader); err == nil || err == io.EOF {
			car.Blocks = append(car.Blocks, block)
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

func (car *Archive[T, R]) Thin(store BlockStore[T]) *ThinArchive[T, R] {
	blocks := make([]Block[T], len(car.Blocks))
	var err error
	for i, block := range car.Blocks {
		if blocks[i], err = store.AddBytes(*block.Id, bytes.NewReader(block.Data)); err != nil {
			panic(err)
		}
	}
	return &ThinArchive[T, R]{car.Header, blocks}
}

type ThinArchive[T BlockId, R BlockIdRef[T]] struct {
	Header ArchiveHeader[T] `json:"hdr"`
	Blocks []Block[T]       `json:"blocks"`
}

func (car *ThinArchive[T, R]) Write(writer io.Writer) error {
	if err := car.Header.Write(writer); err == nil {
		for _, block := range car.Blocks {
			idAsBytes, err := block.Id().MarshalBinary()
			if err != nil {
				return err
			}
			err = writeUvarint(writer, uint64(block.Size()+int64(len(idAsBytes))))
			if err != nil {
				return err
			}
			_, err = writer.Write(idAsBytes)
			if err != nil {
				return err
			}
			_, err = block.Write(writer)
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		return err
	}
}

func (car *ThinArchive[T, R]) Read(store BlockStore[T], reader ByteAndBlockReader) error {
	var err error
	err = car.Header.Read(reader)
	car.Blocks = car.Blocks[:0]
	for err == nil {
		var size uint64
		if size, err = binary.ReadUvarint(reader); err == nil {
			return err
		}
		blockreader := bufio.NewReader(io.LimitReader(reader, int64(size)))
		var id R
		if _, err = id.Read(blockreader); err != nil {
			return err
		}
		var block Block[T]
		if block, err = store.AddBytes(*id, blockreader); err == nil {
			car.Blocks = append(car.Blocks, block)
		}
	}
	return err
}

func (car *ThinArchive[T, R]) Fat() *Archive[T, R] {
	blocks := make([]BlockWireFormat[T, R], len(car.Blocks))
	for i, block := range car.Blocks {
		id := block.Id()
		blocks[i] = BlockWireFormat[T, R]{&id, block.Bytes()}
	}
	return &Archive[T, R]{car.Header, blocks}
}

type BlocksMessage[T BlockId, R BlockIdRef[T]] struct {
	Car Archive[T, R]
}

func (msg *BlocksMessage[T, B]) Write(writer io.Writer) error {
	return msg.Car.Write(writer)
}

func (msg *BlocksMessage[T, B]) Read(reader ByteAndBlockReader) error {
	return msg.Car.Read(reader)
}

func (ah *BlocksMessage[T, B]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

func (msg *BlocksMessage[T, B]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}

type ThinBlocksMessage[T BlockId, R BlockIdRef[T]] struct {
	Car ThinArchive[T, R]
}

func (msg *ThinBlocksMessage[T, B]) Write(writer io.Writer) error {
	return msg.Car.Write(writer)
}

func (msg *ThinBlocksMessage[T, B]) Read(store BlockStore[T], reader ByteAndBlockReader) error {
	return msg.Car.Read(store, reader)
}
