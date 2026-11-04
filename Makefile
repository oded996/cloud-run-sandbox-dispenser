.PHONY: build test install clean

build:
	go build -trimpath -ldflags="-s -w" -o sbx .

test:
	go test -v ./...

install: build
	@BIN_DIR="$${GOBIN:-$$(go env GOPATH | cut -d: -f1)/bin}" && \
	mkdir -p "$$BIN_DIR" && \
	cp -f sbx "$$BIN_DIR/sbx" && \
	chmod 0755 "$$BIN_DIR/sbx" && \
	echo "Installed sbx to $$BIN_DIR/sbx"

clean:
	rm -f sbx dispenser
