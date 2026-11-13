SHELL := bash
.SHELLFLAGS := -eo pipefail -c
.DEFAULT_GOAL := verify
RELEASE_RUNNER ?= scripts/release/release.py
RELEASE_SKIP_TESTS ?= false
RELEASE_VERSION ?=
export RELEASE_SKIP_TESTS RELEASE_VERSION

.PHONY: verify test-release-signing release-check-user release-check-ci
.PHONY: release-prepare release release-dry-run _release-prepare _release _release-dry-run
.PHONY: ci-release-prepare ci-release ci-release-dry-run _ci-release-prepare _ci-release _ci-release-dry-run

verify:
	mvn -ntp verify

release-check-user:
	python3 "$(RELEASE_RUNNER)" check-user

release-check-ci:
	python3 "$(RELEASE_RUNNER)" check-ci

release-prepare _release-prepare:
	python3 "$(RELEASE_RUNNER)" prepare

release _release:
	python3 "$(RELEASE_RUNNER)" perform

release-dry-run _release-dry-run:
	python3 "$(RELEASE_RUNNER)" dry-run

ci-release-prepare _ci-release-prepare:
	python3 "$(RELEASE_RUNNER)" prepare --ci

ci-release _ci-release:
	python3 "$(RELEASE_RUNNER)" perform --ci

ci-release-dry-run _ci-release-dry-run:
	python3 "$(RELEASE_RUNNER)" dry-run --ci

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v
