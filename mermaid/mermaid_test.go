package mermaid

import (
	"fmt"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestStateDiagram(t *testing.T) {
	observedZapCore, observedLogs := observer.New(zap.InfoLevel)
	observedLogger := zap.New(observedZapCore)

	d := OpenStateDiagram("test", observedLogger.Sugar())
	d.LogTransition("state1", "state2", "event1")
	d.LogTransition("state2", "state3", "event2")
	d.LogTransition("state3", "state4", "event3")
	// Duplicate line, which should be ignored
	d.LogTransition("state3", "state4", "event3")
	d.Close()

	// 6 lines - 1 for opening, 1 for stateDiagram-v2, 3 for state changes, 1 for closing
	if observedLogs.Len() != 6 {
		t.Errorf("Expected 6 log entries, got %d", observedLogs.Len())
	}

	line1 := observedLogs.All()[0]
	if line1.Message != "```mermaid" {
		t.Errorf("Expected ```mermaid, got %s", line1.Message)
	}

	if line1.Context[0].Key != "entity" {
		t.Errorf("Expected entity, got %s", line1.Context[0].Key)
	}

	if line1.Context[0].String != "test" {
		t.Errorf("Expected test, got %s", line1.Context[0].String)
	}

	if line1.Context[1].Key != "event" {
		t.Errorf("Expected event, got %s", line1.Context[1].Key)
	}

	if line1.Context[1].String != "BEGIN" {
		t.Errorf("Expected BEGIN, got %s", line1.Context[1].String)
	}

	line2 := observedLogs.All()[1]
	if line2.Message != "stateDiagram-v2" {
		t.Errorf("Expected stateDiagram-v2, got %s", line2.Message)
	}

	if line2.Context[0].Key != "entity" {
		t.Errorf("Expected entity, got %s", line2.Context[0].Key)
	}

	if line2.Context[0].String != "test" {
		t.Errorf("Expected test, got %s", line2.Context[0].String)
	}

	for i := 2; i < 5; i++ {
		line := observedLogs.All()[i]
		if line.Context[0].Key != "entity" {
			t.Errorf("Expected entity, got %s", line.Context[0].Key)
		}

		if line.Context[0].String != "test" {
			t.Errorf("Expected test, got %s", line.Context[0].String)
		}

		if line.Context[1].Key != "event" {
			t.Errorf("Expected event, got %s", line.Context[1].Key)
		}

		if line.Context[1].String != "LINE" {
			t.Errorf("Expected LINE, got %s", line.Context[1].String)
		}
	}

	line6 := observedLogs.All()[5]
	if line6.Message != "```" {
		t.Errorf("Expected ```, got %s", line6.Message)
	}

	// Print copy/pasteable output, which only displays with -v option
	for i := 0; i < 6; i++ {
		line := observedLogs.All()[i]
		fmt.Println(line.Message)
	}

}
