# builds and tests project via go tools
all:
	@echo "update dependencies"
	go get -u ./...
	go mod tidy
	@echo "build and test"
	go build -v ./...
	go vet ./...
	golangci-lint run ./...
	@echo execute tests on latest go version	
	go test ./...
	go test ./... -race
	@echo execute tests with active lz4 compression on latest go version
	go test ./... -tags=liblz4
	@echo execute tests on older supported go versions
	GOTOOLCHAIN=go1.26.9 go1.26.9 test ./...
	GOTOOLCHAIN=go1.26.9 go1.26.9 test ./... -race
	@echo execute tests on the new go version

#see fsfe reuse tool (https://git.fsfe.org/reuse/tool)
#on linux: if pipx uses outdated packages, delete ~/.local/pipx/cache entries
# [charset-normalizer] extra needed because reuse 6.0 added libmagic as primary
# encoding detector, which is unavailable in a clean macOS pipx venv
	@echo "reuse (license) check"
	pipx run 'reuse[charset-normalizer]' lint

#static code checks:
checks:
	go vet ./...
	golangci-lint run ./...

#escape analysis / heap allocation scan (compiler -m flag)
escape:
	@echo "escape analysis - heap relevant decisions"
	go build -gcflags='github.com/SAP/go-hdb/...=-m=2' ./... 2>&1 | grep -E 'escapes to heap|moved to heap'

#go generate
generate:
	@echo "generate"
	go generate ./...

#install additional tools
tools:
#install stringer
	@echo "install latest stringer version"
	go install golang.org/x/tools/cmd/stringer@latest
#install golangci-lint
	@echo "install latest golangci-lint version"
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

#install additional go versions
go:
	go install golang.org/dl/go1.26.9@latest
	go1.26.9 download
