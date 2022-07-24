package taskcore

import "catapultd/pkg/gen/api"

// Tracker handles tasks for a single job
type Tracker struct {
	unscheduledTasks []*api.TaskSpec
	scheduledTask    map[string]*api.TaskSpec
}

type trackerAction interface {
	isAction()
}

// addTask add task to tracker
type addTask struct {
	spec *api.TaskSpec
}

func (*addTask) isAction() {}

// finTask task finished with result
type finTask struct {
	result *api.TaskResult
}

func (*finTask) isAction() {}

// popTask gets next task from queue to run
type popTask struct {
	result chan *api.TaskSpec
}

func (*popTask) isAction() {}

func (t *Tracker) handleAction(action trackerAction) {
	switch msg := action.(type) {
	case *addTask:
		delete(t.scheduledTask, msg.spec.TaskId)
		t.unscheduledTasks = append(t.unscheduledTasks, msg.spec)

	case *finTask:
		delete(t.scheduledTask, msg.result.TaskId)

	case *popTask:
		// get task to run
		lastIndex := len(t.unscheduledTasks) - 1
		task := t.unscheduledTasks[lastIndex]
		t.unscheduledTasks = t.unscheduledTasks[:lastIndex]
		t.scheduledTask[task.TaskId] = task
		msg.result <- task
	}
}
