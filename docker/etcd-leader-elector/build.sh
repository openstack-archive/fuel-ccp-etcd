#!/usr/bin/bash

GIT_REPO="https://github.com/amnk/contrib"
GIT_BRANCH="election"
CONTRIB_PATH="/golang/src/k8s.io/contrib"

mkdir -p $CONTRIB_PATH

git clone -b $GIT_BRANCH $GIT_REPO $CONTRIB_PATH

export GOPATH=/golang
export CGO_ENABLED=0
export GOOS=linux

cd $CONTRIB_PATH/election/example/ && go get -v
go build -v -a -installsuffix cgo -ldflags '-w' -o /server $CONTRIB_PATH/election/example/main.go
