SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c
.DEFAULT_GOAL := verify
RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= scripts/release/signing.py
RELEASE_LOG_HELPER ?= scripts/release/logs.py
RELEASE_SKIP_TESTS ?= false
RELEASE_VERSION ?=
export RELEASE_SKIP_TESTS RELEASE_VERSION

.PHONY: verify test-release-signing release-validate release-report release-check-ci
.PHONY: ci-release-prepare ci-release ci-release-dry-run

verify:
	mvn -ntp verify

release-check-ci:
	@if [[ "$${GITHUB_ACTIONS:-}" != true ]]; then
		echo "Publication is supported only by the protected CI workflow." >&2
		exit 1
	fi

ci-release-prepare ci-release ci-release-dry-run: release-check-ci

release-validate:
	@python3 "$(RELEASE_GUARD)" check
	python3 "$(RELEASE_SIGNING_HELPER)" maven validate mvn -B --no-transfer-progress -Prelease -Dgpg.skip=true verify

ci-release-prepare:
	@python3 "$(RELEASE_GUARD)" check
	args=(mvn -B --no-transfer-progress clean release:clean release:prepare -DpushChanges=false)
	if [[ -n "$${RELEASE_VERSION}" ]]; then
		if [[ ! "$${RELEASE_VERSION}" =~ ^[A-Za-z0-9][A-Za-z0-9_.+-]*$$ ]]; then
			echo "RELEASE_VERSION must contain only version characters." >&2
			exit 2
		fi
		args+=("-DreleaseVersion=$${RELEASE_VERSION}")
	fi
	baseline=$$(git rev-parse HEAD)
	python3 "$(RELEASE_SIGNING_HELPER)" maven prepare "$${args[@]}"
	python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared "$$baseline"

ci-release ci-release-dry-run:
	@python3 "$(RELEASE_GUARD)" check
	python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	python3 "$(RELEASE_SIGNING_HELPER)" maven $(if $(filter %-dry-run,$@),dry-run,perform) mvn -B --no-transfer-progress release:perform -Drelease.autopublish=$(if $(filter %-dry-run,$@),false,true)

release-report:
	python3 "$(RELEASE_LOG_HELPER)" report

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v
