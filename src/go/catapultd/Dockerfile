FROM golang:1.12.4
RUN apt update \
  && apt install -y git
ADD go.mod /src/go.mod
ADD go.sum /src/go.sum
WORKDIR /src
RUN go mod download
ADD . /src
RUN go build ./cmd/catapult-executor
