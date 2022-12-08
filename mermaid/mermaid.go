package mermaid

import (
	"fmt"
	"sync"

	"go.uber.org/zap"
)

type StateDiagram struct {
	entity   string
	log      *zap.SugaredLogger
	lines    []string
	linesMap map[string]bool
	mutex    sync.RWMutex
}

func OpenStateDiagram(entity string, log *zap.SugaredLogger) *StateDiagram {
	line := "stateDiagram-v2"

	d := &StateDiagram{
		entity:   entity,
		log:      log.With("entity", entity),
		lines:    []string{line},
		linesMap: make(map[string]bool),
		mutex:    sync.RWMutex{},
	}

	d.linesMap[line] = true
	d.LogBegin()
	d.Log(line)
	return d
}

func (d *StateDiagram) Log(line string) {
	d.log.With("event", "LINE").Infof(line)
}

func (d *StateDiagram) LogBegin() {
	d.log.With("event", "BEGIN").Infof("```mermaid")
}

func (d *StateDiagram) LogEnd() {
	d.log.With("event", "END").Infof("```")
}

func (d *StateDiagram) LogTransition(event string, fromState string, toState string) {
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
	d.lines = append(d.lines, line)
	d.Log(line)
}

func (d *StateDiagram) Close() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.LogEnd()

	d.log.Sync()
}
