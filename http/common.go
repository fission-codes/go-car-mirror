package http

import (
	"errors"

	golog "github.com/ipfs/go-log/v2"
)

var log = golog.Logger("http")

var (
	ErrInvalidSession  = errors.New("invalid session")
	ErrInvalidResponse = errors.New("invalid http response")
)
