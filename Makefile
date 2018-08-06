pwd= $(shell pwd)

build.%:
	@$(eval name=$(subst build.,,$@))
	@CGO_ENABLED=0 GOOS=linux go build -a -o build/$(name)/main cmd/$(name)/main.go
	@docker build . -t kafka-ctl

launch:
	@docker run -ti --network host -v $(pwd)/build:/binaries kafka-ctl:latest sh

