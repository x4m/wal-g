#!/bin/sh
set -e -x

if ! which dep > /dev/null; then
    go get -u github.com/golang/dep/cmd/dep  # install dependencies management system
fi

make deps  # install dependencies

make
docker-compose build
docker-compose up --exit-code-from pg pg
