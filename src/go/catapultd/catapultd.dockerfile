FROM golang:1.12.5-alpine3.9 as builder
RUN apk update && apk add git
ADD go.mod /src/go.mod
ADD go.sum /src/go.sum
WORKDIR /src
RUN go mod download
ADD . /src
RUN go build

FROM alpine:3.9
COPY --from=builder /src/catapultd /usr/local/bin
