package fixtures

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math/rand"

	"github.com/fxamacker/cbor/v2"
)

func RandId() MockBlockId {
	var id MockBlockId
	rand.Read(id[:])
	return id
}

func RandMockBlock() *MockBlock {
	id := RandId()
	return NewMockBlock(id, int64(rand.Intn(10240)))
}

// BlockId
type MockBlockId [32]byte

func (id MockBlockId) String() string {
	return base64.URLEncoding.EncodeToString(id[:])
}

func (id MockBlockId) MarshalBinary() ([]byte, error) {
	return id[:], nil
}

func (id *MockBlockId) UnmarshalBinary(bytes []byte) error {
	if len(bytes) < 32 {
		return errors.New("bad size for id")
	}
	copy(id[:], bytes)
	return nil
}

func (id MockBlockId) MarshalCBOR() ([]byte, error) {
	if bytes, error := id.MarshalBinary(); error == nil {
		return cbor.Marshal(bytes)
	} else {
		return nil, error
	}
}

func (id *MockBlockId) UnmarshalCBOR(bytes []byte) error {
	var rawbytes []byte
	if error := cbor.Unmarshal(bytes, &rawbytes); error == nil {
		return id.UnmarshalBinary(rawbytes)
	} else {
		return error
	}
}

type MockIdJsonFormat struct {
	Id []byte `json:"id"`
}

func (id MockBlockId) MarshalJSON() ([]byte, error) {
	return json.Marshal(MockIdJsonFormat{Id: id[:]})
}

func (id *MockBlockId) UnmarshalJSON(bytes []byte) error {
	var data MockIdJsonFormat
	if err := json.Unmarshal(bytes, &data); err == nil {
		copy(id[:], data.Id)
		return nil
	} else {
		return err
	}
}

func (id *MockBlockId) Read(reader io.ByteReader) (int, error) {
	var err error
	for i := 0; i < 32 && err == nil; i++ {
		id[i], err = reader.ReadByte()
	}
	return 32, err
}

// Block
type MockBlock struct {
	id    MockBlockId
	links []MockBlockId
	size  int64
}

func NewMockBlock(id MockBlockId, size int64) *MockBlock {
	return &MockBlock{
		id:    id,
		links: make([]MockBlockId, 0, 10),
		size:  size,
	}
}

func (b *MockBlock) Id() MockBlockId {
	return b.id
}

func (b *MockBlock) Write(writer io.Writer) (int64, error) {
	// Deterministic seed; hopefully always generate same content from same id
	source := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(b.id[0:8]) >> 1)))
	return io.CopyN(writer, source, b.size)

}

func (b *MockBlock) Bytes() []byte {
	var buf bytes.Buffer
	b.Write(&buf)
	return buf.Bytes()
}

func (b *MockBlock) Size() int64 {
	return b.size
}

func (b *MockBlock) Children() []MockBlockId {
	return b.links
}

func (b *MockBlock) AddChild(id MockBlockId) error {
	b.links = append(b.links, id)

	return nil
}
