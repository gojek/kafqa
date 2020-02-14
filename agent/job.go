package agent

type JobFn func() error
type BgJob struct {
	run func() error
	id  string
}

func (j BgJob) ID() string { return j.id }

func (j BgJob) Run() error { return j.run() }

func NewJob(id string, fn JobFn) Job {
	return BgJob{run: fn, id: id}
}
