package catapult

import (
	"go.uber.org/zap"
	"time"
)

// Planner globally plans tasks
type Planner struct {
	clients  map[ClientID]*ClientTracker
	incoming chan plannerEvent
	backend  Backend
	logger   *zap.Logger
}

func NewPlanner(backend Backend, logger *zap.Logger) *Planner {
	return &Planner{
		clients:  make(map[ClientID]*ClientTracker),
		incoming: make(chan plannerEvent, 1),
		backend:  backend,
		logger:   logger,
	}
}

func (p *Planner) Run() error {
	resizeTimer := time.NewTicker(10 * time.Second)

	for {
		select {
		case event := <-p.incoming:
			p.handle(event)

		case <-resizeTimer.C:
			p.resizeDeployments()

		}
	}

	return nil
}

// resizeDeployments changes deployment size to align with current outstanding tasks
func (p *Planner) resizeDeployments() {
	for _, tracker := range p.clients {
		targetSize := tracker.Tasks.totalTasks - tracker.Tasks.completedTasks
		maxParallelism := int(tracker.Client.JobSpec.Parallelism)
		if targetSize > maxParallelism {
			targetSize = maxParallelism
		}

		p.logger.Info("resize",
			zap.String("client", tracker.Client.ID.String()),
			zap.Int("target", targetSize),
			zap.Int("max", maxParallelism))

		p.backend.SendBackendEvent(&BackendResize{
			ClientID: tracker.Client.ID,
			Size:     targetSize,
		})
	}
}

// Send event to planner for handling
func (p *Planner) Send(event plannerEvent) {
	p.incoming <- event
}

func (p *Planner) handle(rawEvent plannerEvent) {
	switch event := rawEvent.(type) {
	case *ExecutorPollTask:
		p.logger.Info("poll task",
			zap.String("executor", event.ExecutorID),
			zap.String("client", event.ClientID.String()),
		)
		p.handleExecutorPollTask(event)

	case *ExecutorFinTask:
		p.logger.Info("fin task",
			zap.String("executor", string(event.ExecutorID)),
			zap.String("client", event.ClientID.String()),
			zap.String("task", event.Result.TaskId),
		)
		p.handleExecutorFinTask(event)

	case *ExecutorConnect:
		p.handleExecutorConnect(event)

	case *ExecutorDisconnect:
		p.handleExecutorDisconnect(event)

	case *ClientAddTask:
		p.logger.Info("add task",
			zap.String("client", event.ClientID.String()),
			zap.String("task", event.Task.TaskId),
		)
		p.handleClientAddTask(event)

	case *ClientConnect:
		p.handleClientConnect(event.Client)

	case *ClientDisconnect:
		p.handleClientDisconnect(event.Client)
	}
}

func (p *Planner) handleExecutorPollTask(event *ExecutorPollTask) {
	tracker, ok := p.clients[event.ClientID]
	if !ok {
		// client already removed
		return
	}

	executor, ok := tracker.Executors[event.ExecutorID]
	if !ok {
		// executor already removed
		return
	}

	tracker.Tasks.GiveTask(executor)
}

func (p *Planner) handleExecutorFinTask(event *ExecutorFinTask) {
	tracker, ok := p.clients[event.ClientID]
	if !ok {
		// client already removed
		return
	}

	tracker.Tasks.FinTask(event.ExecutorID)
	tracker.Client.Conn.SendResult(event.Result)
}

func (p *Planner) handleExecutorConnect(event *ExecutorConnect) {
	tracker, ok := p.clients[event.Executor.ClientID]
	if !ok {
		// client already removed
		return
	}

	tracker.Executors[event.Executor.ID] = event.Executor

	p.logger.Info("executor connect",
		zap.String("client", event.Executor.ClientID.String()),
		zap.String("executor", string(event.Executor.ID)),
		zap.Int("num-executors", len(tracker.Executors)),
	)
}

func (p *Planner) handleExecutorDisconnect(event *ExecutorDisconnect) {
	tracker, ok := p.clients[event.Executor.ClientID]
	if !ok {
		// client already removed
		return
	}

	tracker.RemoveExecutor(event.Executor.ID)

	p.logger.Info("executor disconnect",
		zap.String("client", event.Executor.ClientID.String()),
		zap.String("executor", string(event.Executor.ID)),
		zap.Int("num-executors", len(tracker.Executors)),
	)
}

func (p *Planner) handleClientAddTask(event *ClientAddTask) {
	tracker, ok := p.clients[event.ClientID]
	if !ok {
		// not tracking this client
		return
	}

	tracker.Tasks.AddTask(event.Task)
}

func (p *Planner) handleClientConnect(c *Client) {
	p.backend.SendBackendEvent(&BackendCreate{
		ClientID: c.ID,
		JobSpec:  c.JobSpec,
	})

	p.clients[c.ID] = NewClientTracker(c, p.backend, p.logger)

	p.logger.Info("client connect",
		zap.String("client", c.ID.String()),
		zap.Int("num-clients", len(p.clients)),
	)
}

func (p *Planner) handleClientDisconnect(c *Client) {
	p.backend.SendBackendEvent(&BackendDelete{ClientID: c.ID})
	delete(p.clients, c.ID)

	p.logger.Info("client disconnect",
		zap.String("client", c.ID.String()),
		zap.Int("num-clients", len(p.clients)),
	)
}
