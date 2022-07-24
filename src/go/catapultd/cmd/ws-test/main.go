package main

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"sync/atomic"
	"time"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	logger.Info("starting")

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	var counter int64

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, req *http.Request) {
		index := atomic.AddInt64(&counter, 1)
		logger := logger.With(zap.Int64("conn", index))
		logger.Info("new request")

		conn, err := upgrader.Upgrade(w, req, nil)
		if err != nil {
			logger.Warn("failed to upgrade", zap.Error(err))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		go handleConn(conn, logger)
	})

	err = http.ListenAndServe(":9999", mux)
	logger.Info("shutting down", zap.Error(err))
}

type Request struct {
	ID string `json:"task_id"`
}

type Response struct {
	ID string `json:"id"`
}

func handleConn(ws *websocket.Conn, logger *zap.Logger) {
	defer ws.Close()

	ws.SetPingHandler(func(data string) error {
		logger.Info("ping", zap.String("data", data))
		return nil
	})

	requests := make(chan Request, 1000)
	defer close(requests)

	// write loop
	go func() {
		timer := time.NewTicker(1 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				err := ws.WriteControl(websocket.PingMessage, []byte("misc"), time.Now().Add(time.Second*5))
				if err != nil {
					logger.Warn("failed to write control", zap.Error(err))
					return
				}

				select {
				case req := <-requests:
					logger.Info("write response", zap.String("id", req.ID))
					resp := Response{
						ID: req.ID,
					}
					err = ws.WriteJSON(resp)
					if err != nil {
						logger.Warn("failed to write msg", zap.Error(err))
						return
					}
				default:
				}
			}
		}
	}()

	for {
		msgType, p, err := ws.ReadMessage()
		if err != nil {
			logger.Warn("failed to read msg", zap.Error(err))
			return
		}

		if msgType != websocket.TextMessage {
			logger.Warn("unexpected message type", zap.Int("msg-type", msgType))
			return
		}

		logger.Info("recv", zap.String("msg", string(p)))

		var req Request
		dec := json.NewDecoder(bytes.NewReader(p))
		err = dec.Decode(&req)
		if err != nil {
			logger.Warn("failed to decode", zap.Error(err))
			return
		}

		requests <- req
	}
}
