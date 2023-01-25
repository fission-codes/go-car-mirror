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

## Rationale

Car Mirror is a synchronization protocol for directed acyclic graphs (DAGs) which uses Bloom filters to efficiently share information about a graph under synchronization between two parties. Car Mirror is written with IPFS and Kubo in mind, but go-car-mirror itself attempts to minimize dependencies on the existing Kubo codebase. The intent is that go-car-mirror should clearly express the Car Mirror specification in a way that is easily 
portable to other IPFS implementations. 

## Terminology

* Block - a block is a node in the DAG. A block has child blocks which are identifiable via their block ids
* Block Id - a unique identifier for a block, typically a IPLD CID
* Block Store - a perisistent store for blocks, allowing retrieval via Block Id
* Source - the block store containing the source graph
* Sink - the block store to which the source graph is to be transferred
* Transfer - an exchange of messages resulting in a graph present on the source being completely copied to the sink
* Client - the party requesting the transfer
* Server - the party fulfilling the request
* Blocks Message - a message sent from Source to Sink containing several blocks
* Status Message - a message sent from Sink to Source containing information about the status of the transfer
* Session - information held by either the Source or Sink relating to a transfer and maintained for (at least) the lifetime of that transfer

## Architecture

 * The `core` package contains interfaces for Block and BlockStore which are generic for BlockId - BlockId may be any compatible type.
   It also contains the SourceSession and SinkSession objects which are responsible for accessing the provided BlockStore objects, 
   queing blocks and status updates for delivery, and processing received blocks in order to generate status updates.
 * The `filter` package contains an interface `Filter` which can represents a bloom filter or a heterogenous collection of bloom filters. 
   `Filter` is used to represent a set of Block Ids known to be present on the Sink
 * The `batch` package adds additional functionality specific to a synchronous, batch based implementation of the protocol. In this
   scenario, blocks from the source are grouped into batches to be sent to the sink. Once a batch is sent, no further blocks are sent
   until the sink has responded with a status update.
 * The `messages` package contains code for serializing lists of Blocks, Block Ids, and Filters into Block and Status messages. The 
   'Blocks' message should be binary compatible with the IPFS CAR file format (hence the name of the project)
 * The `http` package contains a full implementation of the synchronous batch-based version of the protocol over HTTP
 * The `ipld` package contains implementations of the Block and BlockId interfaces which are compatible with Kubo's Node and CID respectively
 * The `fixtures` package contains simple implementations of Block, BlockId, and BlockStore for use in unit tests


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
