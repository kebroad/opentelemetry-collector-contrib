package testutil

import (
	"sync"
	"time"
)

type MockClock struct {
	CurrentTime time.Time
	sync.Mutex
	AdvanceCh chan time.Duration
}

func (m *MockClock) Now() time.Time {
	m.Lock()
	defer m.Unlock()
	return m.CurrentTime
}

func (m *MockClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	go func() {
		for {
			select {
			case advanceBy := <-m.AdvanceCh:
				m.Lock()
				m.CurrentTime = m.CurrentTime.Add(advanceBy)
				m.Unlock()
				if advanceBy > 0 {
					ch <- m.CurrentTime
					return
				}
			}
		}
	}()
	return ch
}

func (m *MockClock) Advance(d time.Duration) {
	m.AdvanceCh <- d
}

func (m *MockClock) Set(t time.Time) {
	m.Lock()
	m.CurrentTime = t
	m.Unlock()
}

func (m *MockClock) Add(d time.Duration) {
	m.Lock()
	m.CurrentTime = m.CurrentTime.Add(d)
	m.Unlock()
}
