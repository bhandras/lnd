package clock

import (
	"time"
)

// DefaultClock implements Clock interface by simply calling the appropriate
// time functions.
type DefaultClock struct{}

// NewDefaultClock constructs a new DefaultClock.
func NewDefaultClock() Clock {
	return &DefaultClock{}
}

// Now simply returns time.Now().
func (DefaultClock) Now() time.Time {
	return time.Now()
}
