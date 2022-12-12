package diagram

import (
	"fmt"
	"io"
	"sync"

	"github.com/fission-codes/go-car-mirror/carmirror"
	golog "github.com/ipfs/go-log/v2"
	"go.uber.org/zap"
)

var log = golog.Logger("diagram")

type StateDiagrammer struct {
	entity   string
	lines    []string
	linesMap map[string]bool
	mutex    sync.RWMutex
}

func NewStateDiagrammer(entity string, log *zap.SugaredLogger) *StateDiagrammer {
	d := &StateDiagrammer{
		entity:   entity,
		lines:    make([]string, 0),
		linesMap: make(map[string]bool),
		mutex:    sync.RWMutex{},
	}

	d.begin()
	return d
}

func (d *StateDiagrammer) addLine(line string) {
	d.lines = append(d.lines, line)
}

func (d *StateDiagrammer) begin() {
	d.addLine("```mermaid")
	d.addLine("stateDiagram-v2")
}

func (d *StateDiagrammer) end() {
	d.addLine("```")
}

func (d *StateDiagrammer) Transition(event string, fromState string, toState string) {
	if fromState == "" {
		fromState = "[*]"
	}
	line := fmt.Sprintf("  %s --> %s: %s", fromState, toState, event)
	d.mutex.RLock()
	if _, ok := d.linesMap[line]; ok {
		d.mutex.RUnlock()
		return
	}
	d.mutex.RUnlock()

	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.linesMap[line] = true
	d.addLine(line)
}

func (d *StateDiagrammer) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.end()
}

func (d *StateDiagrammer) Write(w io.Writer) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	fmt.Fprintf(w, "## %s\n\n", d.entity)
	for _, line := range d.lines {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w)
}

type DiagrammedBatchSendOrchestrator struct {
	orchestrator *carmirror.BatchSendOrchestrator
	diagrammer   *StateDiagrammer
}

func NewDiagrammedBatchSendOrchestrator(orchestrator *carmirror.BatchSendOrchestrator, diagrammer *StateDiagrammer) *DiagrammedBatchSendOrchestrator {

	return &DiagrammedBatchSendOrchestrator{
		orchestrator: orchestrator,
		diagrammer:   diagrammer,
	}
}

func (do *DiagrammedBatchSendOrchestrator) Notify(event carmirror.SessionEvent) error {
	// TODO: diagram
	fromState := do.orchestrator.State()
	err := do.orchestrator.Notify(event)
	toState := do.orchestrator.State()

	do.diagrammer.Transition(event.String(), fromState.String(), toState.String())

	return err
}

func (do *DiagrammedBatchSendOrchestrator) State() carmirror.BatchState {
	// TODO: diagram
	result := do.orchestrator.State()

	return result
}

func (do *DiagrammedBatchSendOrchestrator) ReceiveState(state carmirror.BatchState) error {
	// TODO: diagram
	err := do.orchestrator.ReceiveState(state)
	return err
}

func (do *DiagrammedBatchSendOrchestrator) IsClosed() bool {
	// TODO: diagram
	result := do.orchestrator.IsClosed()
	return result
}

type DiagrammedBatchReceiveOrchestrator struct {
	orchestrator *carmirror.BatchReceiveOrchestrator
	diagrammer   *StateDiagrammer
}

func NewDiagrammedBatchReceiveOrchestrator(orchestrator *carmirror.BatchReceiveOrchestrator, diagrammer *StateDiagrammer) *DiagrammedBatchReceiveOrchestrator {

	return &DiagrammedBatchReceiveOrchestrator{
		orchestrator: orchestrator,
		diagrammer:   diagrammer,
	}
}

func (do *DiagrammedBatchReceiveOrchestrator) Notify(event carmirror.SessionEvent) error {
	// TODO: diagram
	fromState := do.orchestrator.State()
	err := do.orchestrator.Notify(event)
	toState := do.orchestrator.State()

	do.diagrammer.Transition(event.String(), fromState.String(), toState.String())

	return err
}

func (do *DiagrammedBatchReceiveOrchestrator) State() carmirror.BatchState {
	// TODO: diagram
	result := do.orchestrator.State()

	return result
}

func (do *DiagrammedBatchReceiveOrchestrator) ReceiveState(state carmirror.BatchState) error {
	// TODO: diagram
	err := do.orchestrator.ReceiveState(state)
	return err
}

func (do *DiagrammedBatchReceiveOrchestrator) IsClosed() bool {
	// TODO: diagram
	result := do.orchestrator.IsClosed()
	return result
}
