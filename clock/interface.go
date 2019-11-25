package clock

import (
	"time"
)

// Clock is an interface that provides a time functions for LND packages.
// This is useful during testing when a concrete time reference is needed.
type Clock interface {
	Now() time.Time
}
