all: clean check-quality build testv golangci

ALL_PACKAGES=$(shell go list ./...)
SOURCE_DIRS=$(shell go list ./... | cut -d "/" -f4 | uniq)

clean:
	rm -rf ./out
	GO111MODULE=on go mod tidy -v

setup:
	GO111MODULE=off go get -v golang.org/x/tools/cmd/goimports
	GO111MODULE=off go get -v golang.org/x/lint/golint

setup_godog:
	go get -v github.com/DATA-DOG/godog/cmd/godog

check-quality: setup lint fmt imports vet

build-linux:
	GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -o out/agent-lx ./cmd/agent

build-local:
	@echo "Building './out/kafqa' './out/agent'..."
	@mkdir -p ./out
	@go build -o out/kafqa ./cmd/kafqa
	@go build -o out/agent ./cmd/agent

build: build-linux build-local

testv:
	GOMAXPROCS=1 go test -race -v ./...

test:
	go test -race ./...

testcodecov:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...

fmt:
	gofmt -l -s -w $(SOURCE_DIRS)

imports:
	./scripts/lint.sh check_imports

fix_imports:
	goimports -l -w .

golangci:
	GO111MODULE=off go get -v github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run -v --deadline 5m0s

cyclo:
	gocyclo -over 6 $(SOURCE_DIRS)

vet:
	go vet ./...

test-coverage:
	mkdir -p ./out
	@echo "mode: count" > coverage-all.out
	$(foreach pkg, $(ALL_PACKAGES),\
	go test -coverprofile=coverage.out -covermode=count $(pkg);\
	tail -n +2 coverage.out >> coverage-all.out;)
	GO111MODULE=on go tool cover -html=coverage-all.out -o ./out/coverage.html
	@echo "---"
	cat ./out/coverage.html | grep "<option" | cut -d ">" -f2 | cut -d "<" -f1 | grep -v "mock" | grep -v "config" | grep -v "stub"

docker-rebuild:
	docker-compose up -d --build

docker-force-rebuild:
	docker-compose up -d --build --no-cache

docker-services-restart:
	docker-compose stop kafka
	docker-compose restart zookeeper
	docker-compose start kafka
	docker-compose restart statsd

run: build
	./out/kafqa

lint:
	@if [[ `golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; } | wc -l | tr -d ' '` -ne 0 ]]; then \
          golint $(ALL_PACKAGES) | { grep -vwE "exported (var|function|method|type|const) \S+ should have comment" || true; }; \
          exit 2; \
    fi;

