package store

import "github.com/stretchr/testify/mock"

type InMemoryStoreMock struct {
	mock.Mock
}

func (m *InMemoryStoreMock) Acknowledge(msg Trace) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *InMemoryStoreMock) Track(msg Trace) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *InMemoryStoreMock) Unacknowledged() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *InMemoryStoreMock) Result() Result {
	args := m.Called()
	return args.Get(0).(Result)
}
