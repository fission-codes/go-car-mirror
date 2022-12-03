package errors

import "errors"

var ErrBlockNotFound = errors.New("block not found")
var ErrStatsAlreadyInitialized = errors.New("stats already initialized")
var ErrStateError = errors.New("unexpected state transition")
