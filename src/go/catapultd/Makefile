
NAME = catapultd
TAG = latest
PROJECT = myproject
IMAGE = gcr.io/$(PROJECT)/$(NAME):$(TAG)

.PHONY: all
## all: build
all:
	go build

.PHONY: proto
## proto: generate protobuf files
proto:
	protoc -I protos/catapult api.proto --go_out=plugins=grpc:pkg/gen/api

.PHONY: format
## format: run formatter
format:
	gofmt -l -w .

## image: build image
.PHONY: image
image:
	docker build -f catapultd.dockerfile -t $(IMAGE) .

## push: push image
.PHONY: push
push: image
	docker push $(IMAGE)

## deploy: deploy new scheduler to k8s
.PHONY: deploy
deploy: push
	kubectl apply -f k8s/
	kubectl rollout restart deploy/catapultd

.PHONY: help
## help: prints this help message
#
# derived from this blog post:
# https://danishpraka.sh/2019/12/07/using-makefiles-for-go.html
help:
	@echo "Usage:"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'
