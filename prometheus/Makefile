# builds and tests project via go tools
all:
	@echo "update dependencies"
	go get -u ./...
	go mod tidy
	@echo "build and test"
	go build -v ./...
	$(MAKE) checks
	@echo execute tests on latest go version	
	go test ./...
#	@echo execute tests on older supported go versions
	GOTOOLCHAIN=go1.26.11 go1.26.11 test ./...
#	@echo execute tests on the new go version

#static code checks:
checks:
	go vet ./...
	golangci-lint run ./...
	govulncheck ./...

#install additional tools
tools:
#install golangci-lint
	@echo "install latest golangci-lint version"
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
#install govulncheck
	@echo "install latest govulncheck version"
	go install golang.org/x/vuln/cmd/govulncheck@latest
