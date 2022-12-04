package carmirror

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

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

func readBufferWithPrefix(reader io.Reader) ([]byte, error) {
	if size, err := binary.ReadUvarint(bufio.NewReader(reader)); err != nil {
		return nil, err
	} else {
		buf := make([]byte, size)
		_, err = io.ReadFull(reader, buf)
		return buf, err
	}
}

type ArchiveHeader[T BlockId] struct {
	Version int
	Roots   []T
}

func (ah *ArchiveHeader[T]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal(ah)
}

func (ah *ArchiveHeader[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(ah)
}

func (ah *ArchiveHeader[T]) UnmarshalCBOR(data []byte) error {
	return cbor.Unmarshal(data, ah)
}

func (ah *ArchiveHeader[T]) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, ah)
}

func (ah *ArchiveHeader[T]) Write(writer io.Writer) error {
	if buf, err := cbor.Marshal(ah); err == nil {
		return writeBufferWithPrefix(writer, buf)
	} else {
		return err
	}
}

func (ah *ArchiveHeader[T]) Read(reader io.Reader) error {
	if buf, err := readBufferWithPrefix(reader); err == nil {
		return ah.UnmarshalCBOR(buf)
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

type Archive[T BlockId, B Block[T]] struct {
	Header ArchiveHeader[T]
	Blocks []B
}

func (car *Archive[T, B]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal(car)
}

func (car *Archive[T, B]) MarshalJSON() ([]byte, error) {
	return json.Marshal(car)
}

func (car *Archive[T, B]) UnmarshalCBOR(bytes []byte) error {
	return cbor.Unmarshal(bytes, car)
}

func (car *Archive[T, B]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, car)
}

func (car *Archive[T, B]) Write(writer io.Writer) error {
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

func (car *Archive[T, B]) Read(reader io.Reader) error {
	if err := car.Header.Read(reader); err == nil {
		var block B // TODO: this is kind of problematic - need a blockstore context
		err = block.Read(reader)
		for err == nil {
			car.Blocks = append(car.Blocks, block)
			// TODO: this might not work for all types? How do we force B to be a struct?
			err = block.Read(reader)
		}
		if err == io.EOF {
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}

func (car *Archive[T, B]) UnmarshalBinary(data []byte) error {
	return car.Read(bytes.NewBuffer(data))
}

func (ah *Archive[T, B]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

type BlocksMessage[T BlockId, B Block[T]] struct {
	BK Archive[T, B]
}

func (msg *BlocksMessage[T, B]) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal(msg)
}

func (msg *BlocksMessage[T, B]) MarshalJSON() ([]byte, error) {
	return json.Marshal(msg)
}

func (msg *BlocksMessage[T, B]) UnmarshalCBOR(bytes []byte) error {
	return cbor.Unmarshal(bytes, msg)
}

func (msg *BlocksMessage[T, B]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, msg)
}

func (msg *BlocksMessage[T, B]) Write(writer io.Writer) error {
	return msg.BK.Write(writer)
}

func (msg *BlocksMessage[T, B]) Read(reader io.Reader) error {
	return msg.BK.Read(reader)
}

func (ah *BlocksMessage[T, B]) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := ah.Write(&buf)
	return buf.Bytes(), err
}

func (msg *BlocksMessage[T, B]) UnmarshalBinary(data []byte) error {
	return msg.Read(bytes.NewBuffer(data))
}
