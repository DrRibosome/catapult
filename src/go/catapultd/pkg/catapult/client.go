package catapult

import (
	"catapultd/pkg/gen/api"
	"github.com/google/uuid"
)

type ClientID = uuid.UUID

// Client connected python client
type Client struct {
	ID      ClientID
	Conn    ClientConnection
	JobSpec *api.JobSpec
}

func NewClient(ID ClientID, Conn ClientConnection, jobSpec *api.JobSpec) *Client {
	return &Client{
		ID:      ID,
		Conn:    Conn,
		JobSpec: jobSpec,
	}
}

// ClientConnection handle
//
// Methods here guarenteed to be called from the same goroutine
type ClientConnection interface {
	SendMsg(msg string) // send message to be displayed client side
	SendResult(result *api.TaskResult)
	Shutdown() // shut down client connection
}
