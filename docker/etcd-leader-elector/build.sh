#!/usr/bin/bash

CONTRIB_PATH="/golang/src/k8s.io/contrib"

export GOPATH=/golang
export CGO_ENABLED=0
export GOOS=linux

cd $CONTRIB_PATH/election/example/ && go get -v
go build -v -a -installsuffix cgo -ldflags '-w' -o /server $CONTRIB_PATH/election/example/main.go
