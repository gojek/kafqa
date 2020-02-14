package agent

import (
	"fmt"
	"sync"
	"time"

	"github.com/gojek/kafqa/config/agent"
	"github.com/gojek/kafqa/logger"
)

type Job interface {
	Run() error
	ID() string
}

type Agent struct {
	jobs   []Job
	timer  *time.Ticker
	errors chan error
	exit   chan struct{}

	wg *sync.WaitGroup
}

func (a *Agent) Start() <-chan error {
	a.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case <-a.timer.C:
				a.runJobs()
			case <-a.exit:
				wg.Done()
				return
			}
		}
	}(a.wg)

	return a.errors
}

func (a *Agent) runJobs() {
	a.wg.Add(len(a.jobs))

	logger.Debugf("running jbs: %d", len(a.jobs))
	for _, j := range a.jobs {

		go func(j Job, wg *sync.WaitGroup) {
			defer wg.Done()

			err := j.Run()
			if err != nil {
				err = fmt.Errorf("unsuccessful job run: %s with error: %v", j.ID(), err)
				select {
				// job shouldn't be blocked when channel is full
				case a.errors <- err:
					logger.Debugf("unsuccessful job run: %s with error: %v", j.ID(), err)
				default:
				}
			} else {
				logger.Infof("job: %s completed successfully", j.ID())
			}
		}(j, a.wg)
	}

}

func (a Agent) Wait() {
	a.wg.Wait()
}

func (a Agent) Stop() {
	a.exit <- struct{}{}
	a.timer.Stop()
	logger.Debugf("[Agent] waiting for goroutines of agents")
	a.wg.Wait()
	close(a.errors)
	close(a.exit)
}

func New(cfg agent.Config, jobs ...Job) *Agent {
	timer := time.NewTicker(cfg.ScheduleDuration())
	logger.Debugf("total jobs registered: %d", len(jobs))
	return &Agent{
		jobs:   jobs,
		timer:  timer,
		exit:   make(chan struct{}, 1),
		wg:     &sync.WaitGroup{},
		errors: make(chan error, 20),
	}
}
