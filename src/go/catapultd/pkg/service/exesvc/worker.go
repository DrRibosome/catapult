package exesvc

import (
	"catapultd/pkg/gen/api"
	"go.uber.org/zap"
	"sync"
	"time"
)

// workerHandler handles worker (executor) connections
type workerHandler struct {
	handle   func(request *api.ExecutorRequest) error // func to call on all received requests
	conn     api.ExecutorService_TasksServer
	outgoing chan *api.ExecutorResponse // messages to be written to executor
	logger   *zap.Logger

	// outstandingTask holds the current task we sent to the executor and
	// are awaiting the result of. If nil, no currently outstanding task.
	outstandingTask *api.TaskSpec

	shutdown func(error)
	done     chan struct{}
	finalErr chan error
}

func (h *workerHandler) Shutdown() {
	/*resp := &api.ExecutorResponse{
		Response: &api.ExecutorResponse_Shutdown{
			Shutdown: &api.Shutdown{},
		},
	}
	h.sendResponse(resp)*/
}

func (h *workerHandler) SendTask(spec *api.TaskSpec) {
	resp := &api.ExecutorResponse{
		Response: &api.ExecutorResponse_TaskSpec{TaskSpec: spec},
	}

	h.sendResponse(resp)
}

// sendResponse queue response to be sent to executor
func (h *workerHandler) sendResponse(resp *api.ExecutorResponse) {
	select {
	case <-h.done:
		return
	case h.outgoing <- resp:
	}
}

func newWorker(handle func(request *api.ExecutorRequest) error, conn api.ExecutorService_TasksServer, logger *zap.Logger) *workerHandler {
	once := new(sync.Once)
	done := make(chan struct{})
	finalErr := make(chan error, 1)
	shutdown := func(err error) {
		once.Do(func() {
			finalErr <- err
			close(done)
		})
	}

	return &workerHandler{
		handle:   handle,
		conn:     conn,
		outgoing: make(chan *api.ExecutorResponse, 1),
		logger:   logger,

		shutdown: shutdown,
		done:     done,
		finalErr: finalErr,
	}
}

func (h *workerHandler) Run() error {
	// NOTE: we dont await the readLoop. Instead, we rely on stream closure
	// to shut down the read loop in the case that we want to disconnect
	// from the server side.

	go func() {
		err := h.readLoop()
		h.logger.Info("read loop shutdown", zap.Error(err))
		h.shutdown(err)
	}()

	go func() {
		err := h.writeLoop()
		h.logger.Info("write loop shutdown", zap.Error(err))
		h.shutdown(err)
	}()

	go func() {
		err := h.heartbeatLoop()
		h.logger.Info("heartbeat loop shutdown", zap.Error(err))
		h.shutdown(err)
	}()

	return <-h.finalErr
}

func (h *workerHandler) heartbeatLoop() error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			resp := &api.ExecutorResponse{
				Response: &api.ExecutorResponse_Heartbeat{
					Heartbeat: &api.Heartbeat{},
				},
			}
			h.sendResponse(resp)

		case <-h.conn.Context().Done():
			return nil
		}
	}
}

func (h *workerHandler) writeLoop() error {
	for {
		select {
		case resp := <-h.outgoing:
			err := h.conn.Send(resp)
			if err != nil {
				return err
			}

		case <-h.conn.Context().Done():
			return nil
		}
	}
}

func (h *workerHandler) readLoop() error {
	// See `Run` method for explaination on why we dont need to handle
	// `done` channel closure
	for {
		// wait for request
		action, err := h.conn.Recv()
		if err != nil {
			return err
		}

		// filter out heartbeat messages
		switch action.Action.(type) {
		case *api.ExecutorRequest_Heartbeat:
			continue
		}

		err = h.handle(action)
		if err != nil {
			return err
		}
	}
}
