package messages

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/filter"
	mock "github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/util"
	"golang.org/x/exp/slices"
)

const MOCK_ID_HASH = 2

func init() {
	filter.RegisterHash(MOCK_ID_HASH, mock.XX3HashBlockId)
}

func assertBytesEqual(a []byte, b []byte, t *testing.T) {
	if len(a) != len(b) {
		t.Errorf("Length ase different: %v, %v", len(a), len(b))
	}
	compare_length := util.Min(len(a), len(b))
	var j int
	for j = 0; j < compare_length && a[j] == b[j]; j++ {
	}
	if j < compare_length {
		t.Errorf("First difference is at byte: %v", j)
	}
}

func TestArchiveHeaderWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	header := ArchiveHeader[mock.BlockId]{1, make([]mock.BlockId, 0)}
	header.Roots = append(header.Roots, mock.RandId())
	header.Roots = append(header.Roots, mock.RandId())
	if err := header.Write(&buf); err != nil {
		t.Errorf("Error writing header, %v", err)
	}
	header2 := ArchiveHeader[mock.BlockId]{}
	if err := header2.Read(&buf); err != nil {
		t.Errorf("Error reading header, %v", err)
	}
	if !reflect.DeepEqual(header, header2) {
		t.Errorf("Headers are no longer equal after transport")
	}
}

func TestBlockWireFormatWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	block := mock.NewBlock(mock.RandId(), 10240)
	rawBlock := CastBlockWireFormat[mock.BlockId](block)
	if err := rawBlock.Write(&buf); err != nil {
		t.Errorf("Problem writing block %v", err)
	}
	assertBytesEqual(block.RawData(), rawBlock.RawData(), t)
	copy := BlockWireFormat[mock.BlockId, *mock.BlockId]{}
	if err := copy.Read(&buf); err != nil {
		t.Errorf("error reading block %v", err)
	}
	if !core.BlockEqual[mock.BlockId](rawBlock, &copy) {
		t.Errorf("Blocks (%v, %v) not equal", rawBlock.Id(), copy.Id())
		assertBytesEqual(rawBlock.RawData(), copy.RawData(), t)
	}
}

func compareBlocks(a []core.RawBlock[mock.BlockId], b []core.RawBlock[mock.BlockId], t *testing.T) {
	if len(a) != len(b) {
		t.Errorf("Lists of blocks are not same length")
	}

	for i, block := range a {
		block2 := b[i]
		if block.Id() != block2.Id() {
			t.Errorf("Ids are not equal for block %v, %v != %v", i, block.Id(), block2.Id())
		}
		data := block.RawData()
		data2 := block2.RawData()
		if !slices.Equal(data, data2) {
			t.Errorf("Byte arrays are not equal for block %v, lengths(%v,%v)", i, len(data), len(data2))

		}
	}
}

func TestArchiveWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	blocks := make([]core.RawBlock[mock.BlockId], 2)
	blocks[0] = mock.RandMockBlock()
	blocks[1] = mock.RandMockBlock()
	roots := make([]mock.BlockId, 2)
	roots[0] = blocks[0].Id()
	roots[1] = blocks[1].Id()
	archive := Archive[mock.BlockId, *mock.BlockId]{}
	archive.Header.Version = 1
	archive.Header.Roots = roots
	archive.Blocks = blocks

	if err := archive.Write(&buf); err != nil {
		t.Errorf("Error writing archive, %v", err)
	}
	archive2 := Archive[mock.BlockId, *mock.BlockId]{}
	if err := archive2.Read(&buf); err != io.EOF {
		t.Errorf("Error reading archive, %v", err)
	}
	if !reflect.DeepEqual(archive.Header, archive2.Header) {
		t.Errorf("Archive Headerrs are no longer equal after transport")
	}
	compareBlocks(archive.Blocks, archive2.Blocks, t)
}

func TestBlocksMessageReadWrite(t *testing.T) {
	buf := bytes.Buffer{}
	blocks := make([]core.RawBlock[mock.BlockId], 1)
	blocks[0] = mock.RandMockBlock()
	message := NewBlocksMessage(uint(23), blocks)

	if err := message.Write(&buf); err != nil {
		t.Errorf("Error writing archive, %v", err)
	}
	message2 := BlocksMessage[mock.BlockId, *mock.BlockId, uint]{}
	if err := message2.Read(&buf); err != io.EOF {
		t.Errorf("Error reading archive, %v", err)
	}
	if message.State != message2.State {
		t.Errorf("State is not same, %v != %v", message.State, message2.State)
	}
	if !reflect.DeepEqual(message.Car.Header, message.Car.Header) {
		t.Errorf("Archive Headers are no longer equal after transport")
	}
	compareBlocks(message.Car.Blocks, message2.Car.Blocks, t)
}

func TestStatusMessageReadWrite(t *testing.T) {
	buf := bytes.Buffer{}
	want := make([]mock.BlockId, 1)
	want[0] = mock.RandId()

	have, err := filter.TryNewBloomFilter[mock.BlockId](1024, MOCK_ID_HASH)
	if err != nil {
		t.Errorf("Error creating bloom filter, %v", err)
	}

	message := NewStatusMessage[mock.BlockId](uint(23), have, want)

	if err := message.Write(&buf); err != nil {
		t.Errorf("Error writing status, %v", err)
	}
	message2 := StatusMessage[mock.BlockId, *mock.BlockId, uint]{}
	if err := message2.Read(&buf); err != nil {
		t.Errorf("Error reading filter, %v", err)
	}
	if message.State != message2.State {
		t.Errorf("State is not same, %v != %v", message.State, message2.State)
	}
	if !message.Have.Any().Equal(message2.Have.Any()) {
		t.Errorf("have lists no longer equal after transport")
	}
	if !slices.Equal(message.Want, message2.Want) {
		t.Errorf("want lists no longer equal after transport")
	}
}
