default: build

build:
	go build -v -ldflags '-s -w' .
