package agent

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gojek/kafqa/config/agent"
	"github.com/gojek/kafqa/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func init() {
	logger.Setup("none")
}

func TestShouldRunJob(t *testing.T) {
	job := new(MockJob)
	job.On("Run").Return(nil)
	job.On("ID").Return("jobid")
	cfg := agent.Config{Agent: agent.Agent{ScheduleMs: 10}}
	agent := New(cfg, job)

	errs := agent.Start()

	time.Sleep(100 * time.Millisecond)
	agent.Stop()

	assert.Empty(t, errs)
	job.AssertExpectations(t)
}

func TestShouldReturnJobErrors(t *testing.T) {
	job := new(MockJob)
	job.On("Run").Return(errors.New("job error"))
	job.On("ID").Return("job-mock-id")
	cfg := agent.Config{Agent: agent.Agent{ScheduleMs: 10}}
	agent := New(cfg, job)

	errs := agent.Start()
	time.Sleep(200 * time.Millisecond)

	agent.Stop()

	assert.True(t, 10 < len(errs), fmt.Sprintf("expected atleast 10 calls: got: %d", len(errs)))
	job.AssertExpectations(t)
}

type MockJob struct{ mock.Mock }

func (m *MockJob) Run() error {
	return m.Called().Error(0)
}

func (m *MockJob) ID() string {
	return m.Called().String(0)
}
