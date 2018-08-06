pwd= $(shell pwd)

build.%:
	@$(eval name=$(subst build.,,$@))
	@CGO_ENABLED=0 GOOS=linux go build -a -o build/$(name)/main cmd/$(name)/main.go
	@echo "successfully built $(name)"

launch:
	@docker run -ti --network host -v $(pwd)/build:/binaries kafka-ctl:latest sh

