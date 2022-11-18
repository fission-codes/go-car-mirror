package errors

import "errors"

var BlockNotFound = errors.New("block not found")
var StatsAlreadyInitialized = errors.New("Stats already initialized")
