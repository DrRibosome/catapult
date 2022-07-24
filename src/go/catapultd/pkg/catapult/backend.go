package catapult

import "catapultd/pkg/gen/api"

type BackendEvent interface {
	isBackendEvent()
}

type Backend interface {
	SendBackendEvent(event BackendEvent)
}

type BackendCreate struct {
	ClientID ClientID
	JobSpec  *api.JobSpec
}

func (*BackendCreate) isBackendEvent() {}

// BackendDelete delete entire backend
type BackendDelete struct {
	ClientID ClientID
}

func (*BackendDelete) isBackendEvent() {}

// BackendDeleteExecutor delete a single executor
type BackendDeleteExecutor struct {
	ClientID   ClientID
	ExecutorID ExecutorID
}

func (*BackendDeleteExecutor) isBackendEvent() {}

type BackendResize struct {
	ClientID ClientID
	Size     int
}

func (*BackendResize) isBackendEvent() {}
