BIN_DIR ?= ./bin
DIRS ?= $(shell find . -name '*.go' | grep --invert-match 'vendor' | xargs -n 1 dirname | sort --unique)
GO_TOOLS := \
	github.com/git-chglog/git-chglog/cmd/git-chglog \
	github.com/mattn/goveralls \

TFLAGS ?=

COVERAGE_PROFILE ?= coverage.out
HTML_OUTPUT      ?= coverage.html

PSQL := $(shell command -v psql 2> /dev/null)

TEST_DATABASE_USER ?= go_pg_migrations_user
TEST_DATABASE_NAME ?= go_pg_migrations

default: install

.PHONY: clean
clean:
	@echo "---> Cleaning"
	go clean

coveralls:
	@echo "---> Sending coverage info to Coveralls"
	goveralls -coverprofile=$(COVERAGE_PROFILE) -service=travis-ci

.PHONY: enforce
enforce:
	@echo "---> Enforcing coverage"
	./scripts/coverage.sh $(COVERAGE_PROFILE)

.PHONY: html
html:
	@echo "---> Generating HTML coverage report"
	go tool cover -html $(COVERAGE_PROFILE) -o $(HTML_OUTPUT)
	open $(HTML_OUTPUT)

.PHONY: install
install:
	@echo "---> Installing dependencies"
	go mod download

.PHONY: lint
lint:
	@echo "---> Linting"
	$(BIN_DIR)/golangci-lint run

.PHONY: release
release:
	@echo "---> Creating new release"
ifndef tag
	$(error tag must be specified)
endif
	git-chglog --output CHANGELOG.md --next-tag $(tag)
	sed -i "" "s/version-.*-green/version-$(tag)-green/" README.md
	git add CHANGELOG.md README.md
	git commit -m $(tag)
	git tag $(tag)
	git push origin master --tags

.PHONY: setup
setup: install
	@echo "--> Setting up"
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(BIN_DIR) v1.16.0
	go get $(GO_TOOLS) && GOBIN=$$(realpath $(BIN_DIR)) go install $(GO_TOOLS)

.PHONY: test
test:
	@echo "---> Testing"
	go test ./... -coverprofile $(COVERAGE_PROFILE) $(TFLAGS)
