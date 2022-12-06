package carmirror

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/fission-codes/go-car-mirror/fixtures"
)

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

func TestArchiveWriteRead(t *testing.T) {
	buf := bytes.Buffer{}
	blocks := make([]Block[fixtures.MockBlockId], 2)
	blocks[0] = fixtures.RandMockBlock()
	blocks[1] = fixtures.RandMockBlock()
	roots := make([]fixtures.MockBlockId, 2)
	roots[0] = blocks[0].Id()
	roots[1] = blocks[1].Id()
	archive := ThinArchive[fixtures.MockBlockId, *fixtures.MockBlockId]{}
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
	if !reflect.DeepEqual(archive.Fat(), archive2) {
		t.Errorf("Archives are no longer equal after transport")
	}
}
