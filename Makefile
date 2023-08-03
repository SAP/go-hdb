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
	@echo execute tests on latest go version	
	go test ./...
	@echo execute tests on older supported go versions
	go1.19.12 test ./...
	@echo execute tests on future go versions
	go1.21rc4 test ./...
#see fsfe reuse tool (https://git.fsfe.org/reuse/tool)
	@echo "reuse (license) check"
	pipx run reuse lint

#go generate
generate:
	@echo "generate"
	go generate ./...

#install additional tools
tools:
#install linter
	@echo "install latest go linter version"
	go install golang.org/x/lint/golint@latest
#install staticcheck
	@echo "install latest staticcheck version"
	go install honnef.co/go/tools/cmd/staticcheck@latest

#install additional go versions
go:
	go install golang.org/dl/go1.19.12@latest
	go1.19.12 download
	go install golang.org/dl/go1.21rc4@latest
	go1.21rc4 download
