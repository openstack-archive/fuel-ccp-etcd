#!/usr/bin/bash

GOPATH=/golang/src/server/vendor:/golang go get
GOPATH=/golang/src/server/vendor:/golang CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-w' -o server /golang/src/server/main.go
