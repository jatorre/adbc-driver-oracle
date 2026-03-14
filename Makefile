BINARY_NAME=libadbc_driver_oracle
GOFLAGS=-buildmode=c-shared

.PHONY: all build test clean

all: build

build:
	go build $(GOFLAGS) -o $(BINARY_NAME).dylib ./cmd/

build-linux:
	GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o $(BINARY_NAME).so ./cmd/

test:
	go test -v ./go/...

clean:
	rm -f $(BINARY_NAME).dylib $(BINARY_NAME).so $(BINARY_NAME).h
