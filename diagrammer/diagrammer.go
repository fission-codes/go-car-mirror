package diagrammer

import (
	"fmt"
	"io"
	"sync"
)

type StateDiagrammer struct {
	name     string
	lines    []string
	linesMap map[string]bool
	mutex    sync.RWMutex
	started  bool
}

func NewStateDiagrammer(name string) *StateDiagrammer {
	d := &StateDiagrammer{
		name:     name,
		lines:    make([]string, 0),
		linesMap: make(map[string]bool),
		mutex:    sync.RWMutex{},
		started:  false,
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
		if d.started {
			fromState = "NO_STATE"
		} else {
			fromState = "[*]"
		}
	}

	if toState == "" {
		toState = "NO_STATE"
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
	d.started = true
}

func (d *StateDiagrammer) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.end()
}

func (d *StateDiagrammer) Write(w io.Writer) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	fmt.Fprintf(w, "## %s\n\n", d.name)
	for _, line := range d.lines {
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w)
}
