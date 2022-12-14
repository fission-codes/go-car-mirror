package errors

import "errors"

// Errors that can be returned by the carmirror package.
var (
	ErrBlockNotFound           = errors.New("block not found")
	ErrStatsAlreadyInitialized = errors.New("stats already initialized")
	ErrStateError              = errors.New("unexpected state transition")
)
