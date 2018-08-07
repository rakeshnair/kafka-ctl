pwd= $(shell pwd)

build.%:
	@$(eval name=$(subst build.,,$@))
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o build/$(name)/main local/$(name)/main.go
	@echo "successfully built $(name)"

docker.launch:
	@docker run -ti --rm --network host -v $(pwd)/build:/binaries kafka-ctl:latest sh

docker.build:
	@docker build . -t kafka-ctl:latest

docker.kafka.start:
	@docker-compose up -d

docker.kafka.stop:
	@docker-compose down
