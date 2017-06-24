#!/bin/bash

set -eu

GOOS=linux GOARCH=amd64 go build -o rmqdump.linux-amd64
gzip -c rmqdump.linux-amd64 >rmqdump.linux-amd64.gz
GOOS=darwin GOARCH=amd64 go build -o rmqdump.darwin-amd64
