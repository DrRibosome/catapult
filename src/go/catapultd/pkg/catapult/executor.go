package catapult

import (
	"catapultd/pkg/gen/api"
)

type ExecutorID = string

type Executor struct {
	ID       ExecutorID
	ClientID ClientID // client this executor serves
	Conn     ExecutorConnection
}

func NewExecutor(id ExecutorID, clientID ClientID, conn ExecutorConnection) *Executor {
	return &Executor{
		ID:       id,
		ClientID: clientID,
		Conn:     conn,
	}
}

// ExecutorConnection provides interface for communcating with the executor
type ExecutorConnection interface {
	Shutdown()                   // order executor to shutdown
	SendTask(spec *api.TaskSpec) // send task for execution
}
