pwd= $(shell pwd)

build.dev.%:
	@$(eval name=$(subst build.dev.,,$@))
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o build/$(name)/main .dev/$(name)/main.go
	@echo "successfully built $(name)"

build.server:
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o build/server/server cmd/main.go
	@echo "successfully built server"

docker.launch:
	@docker run -ti --rm --network host -v $(pwd)/build:/binaries kafka-ctl:latest sh

docker.build:
	@docker build . -t kafka-ctl:latest

docker.kafka.start:
	@docker-compose -f docker-compose-cluster.yml up -d

docker.kafka.stop:
	@docker-compose -f docker-compose-cluster.yml down