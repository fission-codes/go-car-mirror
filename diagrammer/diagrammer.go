package diagrammer

import (
	"fmt"
	"io"
	"sync"
)

type StateDiagrammer struct {
	title    string
	lines    []string
	linesMap map[string]bool
	mutex    sync.RWMutex
	started  bool
	writer   io.Writer
}

func NewStateDiagrammer(title string, writer io.Writer) *StateDiagrammer {
	d := &StateDiagrammer{
		title:    title,
		lines:    make([]string, 0),
		linesMap: make(map[string]bool),
		mutex:    sync.RWMutex{},
		started:  false,
		writer:   writer,
	}

	d.begin()
	return d
}

func (d *StateDiagrammer) addLine(line string) {
	d.lines = append(d.lines, line)
}

func (d *StateDiagrammer) begin() {
	d.addLine("```mermaid")
	d.addLine("---")
	d.addLine(fmt.Sprintf("title: %s", d.title))
	d.addLine("---")
	d.addLine("stateDiagram-v2")
}

func (d *StateDiagrammer) End() {
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

func (d *StateDiagrammer) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.End()
	return d.Write(d.writer)
}

func (d *StateDiagrammer) Write(w io.Writer) error {
	for _, line := range d.lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	return nil
}
