package podbackend

import "catapultd/pkg/gen/api"

// Tracker tracks executors by backend
type Tracker struct {
	NumLaunching    int // number of currently in flight pod launches
	NumLaunchErrors int // number of consecutive launch attempts with errors
	Executors       map[string]bool
	JobSpec         *api.JobSpec
}

func NewTracker(jobSpec *api.JobSpec) *Tracker {
	return &Tracker{
		NumLaunching: 0,
		Executors:    make(map[string]bool),
		JobSpec:      jobSpec,
	}
}

func (t *Tracker) CurrentSize() int {
	return t.NumLaunching + len(t.Executors)
}

// ShouldDelete check if we should delete executor
func (t *Tracker) ShouldDelete(executorID string) bool {
	_, ok := t.Executors[executorID]
	return !ok
}
