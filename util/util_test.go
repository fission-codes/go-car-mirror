package util

import (
	"testing"
)

func TestFlagSetAndContains(t *testing.T) {
	flags := NewSharedFlagSet(uint8(0))
	flags.Set(0x40)
	if !flags.Contains(0x40) {
		t.Fatalf("expected flag set to contain 0x40")
	}
	if flags.Contains(0xBF) {
		t.Fatalf("did not expect flags to contain any other bits")
	}
}

func TestFlagSetWait(t *testing.T) {
	flags := NewSharedFlagSet(uint8(0))
	a := int64(1)

	go func() {
		flags.Set(4)
		flags.Wait(1)
		a += 1
		flags.Unset(4)
	}()

	go func() {
		flags.Set(8)
		flags.Wait(2)
		a *= 2
		flags.Unset(8)

	}()

	flags.Wait(12)
	flags.Set(2)
	flags.WaitExact(8, 0)
	flags.Set(1)
	flags.WaitExact(12, 0)

	if a != 3 {
		t.Fatalf(`expected 1*2+1 = 3, got %d`, a)
	}
}
