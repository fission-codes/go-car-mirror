package http

import (
	"errors"

	"github.com/fission-codes/go-car-mirror/core"
	"github.com/fission-codes/go-car-mirror/core/instrumented"
	"github.com/fission-codes/go-car-mirror/filter"
	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

var (
	ErrInvalidSession  = errors.New("invalid session")
	ErrInvalidResponse = errors.New("invalid http response")
)

type Config struct {
	MaxBatchSize  uint32
	Address       string
	BloomCapacity uint
	BloomFunction uint64
	Instrument    instrumented.InstrumentationOptions
}

func DefaultConfig() Config {
	return Config{
		MaxBatchSize:  32,
		Address:       ":8080",
		BloomCapacity: 1024,
		BloomFunction: 2,
		Instrument:    0,
	}
}

func NewBloomAllocator[I core.BlockId](config *Config) func() filter.Filter[I] {
	return func() filter.Filter[I] {
		filter, err := filter.TryNewBloomFilter[I](config.BloomCapacity, config.BloomFunction)
		if err != nil {
			log.Errorf("Invalid hash function specified %v", config.BloomFunction)
		}
		return filter
	}
}
