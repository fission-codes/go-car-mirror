package http

import (
	"errors"

	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("go-car-mirror")

var (
	ErrInvalidSession  = errors.New("invalid session")
	ErrInvalidResponse = errors.New("invalid http response")
)

type Config struct {
	Address string
}

func DefaultConfig() Config {
	return Config{
		Address: ":8080",
	}
}
