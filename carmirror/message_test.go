package carmirror

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/fission-codes/go-car-mirror/fixtures"
	"github.com/fission-codes/go-car-mirror/util"
	"golang.org/x/exp/slices"
)

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
	header := ArchiveHeader[fixtures.MockBlockId]{1, make([]fixtures.MockBlockId, 0)}
	header.Roots = append(header.Roots, fixtures.RandId())
	header.Roots = append(header.Roots, fixtures.RandId())
	if err := header.Write(&buf); err != nil {
		t.Errorf("Error writing header, %v", err)
	}
	header2 := ArchiveHeader[fixtures.MockBlockId]{}
	if err := header2.Read(&buf); err != nil {
		t.Errorf("Error reading header, %v", err)
	}
	if !reflect.DeepEqual(header, header2) {
		t.Errorf("Headers are no longer equal after transport")
	}
}

func TestBlockWireFormatWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	block := fixtures.NewMockBlock(fixtures.RandId(), 10240)
	rawBlock := CastBlockWireFormat[fixtures.MockBlockId](block)
	if err := rawBlock.Write(&buf); err != nil {
		t.Errorf("Problem writing block %v", err)
	}
	assertBytesEqual(block.Bytes(), rawBlock.Bytes(), t)
	copy := BlockWireFormat[fixtures.MockBlockId, *fixtures.MockBlockId]{}
	if err := copy.Read(&buf); err != nil {
		t.Errorf("error reading block %v", err)
	}
	if !BlockEqual[fixtures.MockBlockId](rawBlock, &copy) {
		t.Errorf("Blocks (%v, %v) not equal", rawBlock.Id(), copy.Id())
		assertBytesEqual(rawBlock.Bytes(), copy.Bytes(), t)
	}
}

func TestArchiveWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	blocks := make([]RawBlock[fixtures.MockBlockId], 2)
	blocks[0] = fixtures.RandMockBlock()
	blocks[1] = fixtures.RandMockBlock()
	roots := make([]fixtures.MockBlockId, 2)
	roots[0] = blocks[0].Id()
	roots[1] = blocks[1].Id()
	archive := Archive[fixtures.MockBlockId, *fixtures.MockBlockId]{}
	archive.Header.Version = 1
	archive.Header.Roots = roots
	archive.Blocks = blocks

	if err := archive.Write(&buf); err != nil {
		t.Errorf("Error writing archive, %v", err)
	}
	archive2 := Archive[fixtures.MockBlockId, *fixtures.MockBlockId]{}
	if err := archive2.Read(&buf); err != io.EOF {
		t.Errorf("Error reading archive, %v", err)
	}
	if !reflect.DeepEqual(archive.Header, archive2.Header) {
		t.Errorf("Archive Headerrs are no longer equal after transport")
	}
	for i, block := range archive.Blocks {
		block2 := archive2.Blocks[i]
		if block.Id() != block2.Id() {
			t.Errorf("Ids are not equal for block %v, %v != %v", i, block.Id(), block2.Id())
		}
		data := block.Bytes()
		data2 := block2.Bytes()
		if !slices.Equal(data, data2) {
			t.Errorf("Byte arrays are not equal for block %v, lengths(%v,%v)", i, len(data), len(data2))

		}
	}
}
