package buffer

import "errors"

var ErrFlushStateInconsistent = errors.New("cannot flush inconsistent buffer")
