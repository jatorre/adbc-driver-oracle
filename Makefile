BINARY_NAME=libadbc_driver_oracle
GOFLAGS=-buildmode=c-shared -tags driverlib

.PHONY: all build test clean

all: build

build:
	go build $(GOFLAGS) -o $(BINARY_NAME).dylib ./pkg/oracle/

build-linux:
	GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o $(BINARY_NAME).so ./pkg/oracle/

# Build CLI test harness (standalone binary, no CGO exports)
build-cli:
	go build -o oracle-adbc-test ./cmd/

test:
	go test -v ./go/...

clean:
	rm -f $(BINARY_NAME).dylib $(BINARY_NAME).so $(BINARY_NAME).h oracle-adbc-test
