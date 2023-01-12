<img src="https://raw.githubusercontent.com/fission-codes/go-car-mirror/main/assets/logo.png" alt="go-car-mirror Logo" height="300"></img>

# go-car-mirror

[![CI](https://github.com/fission-codes/go-car-mirror/actions/workflows/main.yml/badge.svg)](https://github.com/fission-codes/go-car-mirror/actions/workflows/main.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/fission-codes/blob/master/LICENSE)
[![Built by FISSION](https://img.shields.io/badge/⌘-Built_by_FISSION-purple.svg)](https://fission.codes)
[![Discord](https://img.shields.io/discord/478735028319158273.svg)](https://discord.gg/zAQBDEq)
[![Discourse](https://img.shields.io/discourse/https/talk.fission.codes/topics)](https://talk.fission.codes)

🚧 WIP 🚧

Generic Go implementation of [CAR Mirror](https://github.com/fission-codes/spec/tree/main/car-pool).

Will be used in [kubo-car-mirror](https://github.com/fission-codes/kubo-car-mirror) when complete.

## Building

```
make build
```

## Testing

```
make test
```

## Logging

```
export GOLOG_LOG_LEVEL="go-car-mirror=debug"
```


## State Diagrams

These diagrams are generated when running `make test`.  See the output in `testdata/state-diagrams.md`.  Currently you have to copy any markup into this README manually.

### BatchSourceOrchestrator

```mermaid
stateDiagram-v2
  [*] --> NO_STATE: BEGIN_ENQUEUE
  NO_STATE --> SOURCE_PROCESSING: END_ENQUEUE
  SOURCE_PROCESSING --> SOURCE_PROCESSING|SOURCE_CLOSING: BEGIN_CLOSE
  SOURCE_PROCESSING|SOURCE_CLOSING --> SOURCE_PROCESSING|SOURCE_CLOSING: END_CLOSE
  SOURCE_PROCESSING|SOURCE_CLOSING --> SOURCE_PROCESSING|SOURCE_CLOSING: BEGIN_SESSION
  SOURCE_PROCESSING|SOURCE_CLOSING --> SOURCE_PROCESSING|SOURCE_CLOSING: BEGIN_SEND
  SOURCE_PROCESSING|SOURCE_CLOSING --> SOURCE_PROCESSING|SOURCE_CLOSING: END_SEND
  SOURCE_PROCESSING|SOURCE_CLOSING --> SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_DRAINING
  SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_FLUSH
  SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_CLOSING|SOURCE_CLOSED: END_FLUSH
  SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_CLOSING|SOURCE_CLOSED: END_DRAINING
  SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_CLOSING|SOURCE_CLOSED: END_SEND
  SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_RECEIVE
  SOURCE_CLOSING|SOURCE_CLOSED --> SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: END_RECEIVE
  SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: ReceiveState
  SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_SEND
  SINK_CLOSED|SINK_SENDING|SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_DRAINING
  SINK_CLOSED|SINK_SENDING|SOURCE_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_FLUSH
  SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED: END_FLUSH
  SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED: END_DRAINING
  SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED: END_SEND
  SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSED: END_SESSION
```

### BatchSinkOrchestrator

```mermaid
stateDiagram-v2
  [*] --> NO_STATE: BEGIN_SESSION
  NO_STATE --> NO_STATE: BEGIN_BATCH
  NO_STATE --> NO_STATE: BEGIN_RECEIVE
  NO_STATE --> NO_STATE: END_RECEIVE
  NO_STATE --> SOURCE_CLOSING|SOURCE_CLOSED: ReceiveState
  SOURCE_CLOSING|SOURCE_CLOSED --> SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: END_BATCH
  NO_STATE --> SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_CHECK
  SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: END_CHECK
  SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_CHECK
  SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_SEND
  SINK_CLOSED|SINK_SENDING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED: END_SEND
  SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED: END_CHECK
  SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED: END_SESSION
  SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED: BEGIN_BATCH
  SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED: ReceiveState
  SINK_CLOSED|SINK_WAITING|SOURCE_CLOSING|SOURCE_CLOSED --> SINK_CLOSED|SINK_PROCESSING|SOURCE_CLOSING|SOURCE_CLOSED: END_BATCH
```
