CMD_FILES = $(wildcard cmd/wal-g/*.go)
PKG_FILES = $(wildcard *.go)

.PHONY : test install all clean

test: cmd/x4m/wal-g
	go test -v

all: cmd/x4m/wal-g	

install:
	(cd cmd/wal-g && go install)

clean:
	rm -r extracted compressed $(wildcard data*)
	go clean
	(cd cmd/wal-g && go clean)

cmd/x4m/wal-g: $(CMD_FILES) $(PKG_FILES)
	(cd cmd/wal-g && go build)
