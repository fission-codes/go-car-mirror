package util

import (
	"testing"
)

// TestHelloName calls greetings.Hello with a name, checking
// for a valid return value.
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
