# builds and tests project via go tools
all:
	@echo "update dependencies"
	go get -u ./...
	go mod tidy
	@echo "build and test"
	go build -v ./...
	go vet ./...
	golint -set_exit_status=true ./...
	staticcheck -checks all -fail none ./...
	golangci-lint run ./...
	@echo execute tests on latest go version	
	go test ./...
	go test ./... -race
#	@echo execute tests on older supported go versions
	GOTOOLCHAIN=go1.24.12 go1.24.12 test ./...
	GOTOOLCHAIN=go1.24.12 go1.24.12 test ./... -race
#	@echo execute tests on the new go version
	GOTOOLCHAIN=go1.26rc2 go1.26rc2 test ./...
	GOTOOLCHAIN=go1.26rc2 go1.26rc2 test ./... -race

#see fsfe reuse tool (https://git.fsfe.org/reuse/tool)
#on linux: if pipx uses outdated packages, delete ~/.local/pipx/cache entries
	@echo "reuse (license) check"
	pipx run reuse lint

#go generate
generate:
	@echo "generate"
	go generate ./...

#install additional tools
tools:
#install stringer
	@echo "install latest stringer version"
	go install golang.org/x/tools/cmd/stringer@latest
#install linter
	@echo "install latest go linter version"
	go install golang.org/x/lint/golint@latest
#install staticcheck
	@echo "install latest staticcheck version"
	go install honnef.co/go/tools/cmd/staticcheck@latest
#install golangci-lint
	@echo "install latest golangci-lint version"
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

#install additional go versions
go:
	go install golang.org/dl/go1.24.12@latest
	go1.24.12 download
	go install golang.org/dl/go1.26rc2@latest
	go1.26rc2 download
