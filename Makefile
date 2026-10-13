GO ?= go
PKGS := ./...

.PHONY: all fmt vet lint test race cover bench fuzz tidy check tags

all: check

fmt:
	gofmt -s -w .

vet:
	$(GO) vet $(PKGS)

lint:
	golangci-lint run $(PKGS)

test:
	$(GO) test -count=1 $(PKGS)

race:
	$(GO) test -race -count=1 $(PKGS)

cover:
	$(GO) test -race -count=1 -coverprofile=coverage.out -covermode=atomic $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

bench:
	$(GO) test -run=^$$ -bench=. -benchmem $(PKGS)

fuzz:
	$(GO) test -run=^$$ -fuzz=FuzzSQLVerb -fuzztime=30s .
	$(GO) test -run=^$$ -fuzz=FuzzPrettySQL -fuzztime=30s .

tidy:
	$(GO) mod tidy

check: vet race
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

tags:
	@bash -c ' \
		version=$$(cat "$(CURDIR)/version" 2>/dev/null || echo "0.0.0") && \
		echo "→ work with current directory and $$version" && \
		tag=v$$version && \
		echo "→ tag: $$tag" && \
		if [[ ! $$(git tag -l "$$tag") ]]; then \
			git tag -a "$$tag" -m "Release $$version" && \
			git push origin "$$tag" -o ci.skip && \
			echo "✅ Tagged and pushed $$tag"; \
		else \
			echo "⚠️  Tag $$tag already exists"; \
		fi \
	'