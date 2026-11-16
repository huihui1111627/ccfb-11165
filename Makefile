.PHONY: build seed server bench test report clean

build:
	go build -o bin/server ./cmd/server
	go build -o bin/seed ./cmd/seed
	go build -o bin/bench ./cmd/bench

seed: build
	./bin/seed -rows 30000000 -years 7

server: build
	./bin/server -listen :18080

bench: build
	./bin/bench -phase pre

test:
	go test ./...

clean:
	rm -rf bin data
