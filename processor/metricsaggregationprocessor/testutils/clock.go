package testutils

import (
	"time"
)

type mockClock struct {
    currentTime time.Time
}

func (m *mockClock) Now() time.Time {
    return m.currentTime
}

func (m *mockClock) After(d time.Duration) <-chan time.Time {
    m.currentTime = m.currentTime.Add(d)
    return time.After(0) // immediately return
}

func (m *mockClock) Set(t time.Time) {
    m.currentTime = t
}

func (m *mockClock) Add(d time.Duration) {
    m.currentTime = m.currentTime.Add(d)
}