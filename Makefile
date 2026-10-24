SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c
.DEFAULT_GOAL := verify
RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= $(dir $(RELEASE_GUARD))signing.py
RELEASE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
mvn = mvn -ntp
MAVEN_OPTS ?=
RELEASE_SKIP_TESTS ?= false
RELEASE_VERSION ?=
# GNU make normalizes -n/--just-print/--dry-run to n in its short flag word.
RELEASE_MAKE_PREVIEW = $(findstring n,$(filter-out --%,$(firstword $(MAKEFLAGS))))

.PHONY: verify release-check-user release-check-ci release-prepare release release-dry-run test-release-signing _release-prepare _release _release-dry-run
.PHONY: ci-release-prepare ci-release ci-release-dry-run _ci-release-prepare _ci-release _ci-release-dry-run
verify:
	${mvn} verify

release-check-user:
	@set -eu
	if ! user=$$(gh api user --jq .login); then
		echo "Release requires GitHub user scylladb-publisher. Run gh auth login or gh auth switch." >&2
		exit 1
	fi
	if [ "$$user" != scylladb-publisher ]; then
		echo "Release requires GitHub user scylladb-publisher (current: $$user)." >&2
		exit 1
	fi

release-check-ci:
	@if [ "$${GITHUB_ACTIONS:-}" != true ]; then
		echo "Use local release targets outside GitHub Actions." >&2
		exit 1
	fi

release-prepare release release-dry-run _release-prepare _release _release-dry-run: release-check-user
ci-release-prepare ci-release ci-release-dry-run _ci-release-prepare _ci-release _ci-release-dry-run: release-check-ci

release-prepare ci-release-prepare:
ifneq ($(RELEASE_MAKE_PREVIEW),)
	+@$(MAKE) -f "$(RELEASE_MAKEFILE)" _$@
else
	@python3 "$(RELEASE_GUARD)" check
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _$@

endif

release ci-release release-dry-run ci-release-dry-run:
ifneq ($(RELEASE_MAKE_PREVIEW),)
	+@$(MAKE) -f "$(RELEASE_MAKEFILE)" _$@
else
	@python3 "$(RELEASE_GUARD)" check
	@python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	+@python3 "$(RELEASE_SIGNING_HELPER)" exec $(MAKE) -f "$(RELEASE_MAKEFILE)" _$@

endif

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v

_release-prepare _ci-release-prepare:
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	NEXT_RELEASE=()
	if [[ -n "$${RELEASE_VERSION}" ]]; then
		NEXT_RELEASE=("-DreleaseVersion=$${RELEASE_VERSION}")
	fi
	export MAVEN_OPTS
	baseline=$$(git rev-parse HEAD)
	python3 "$(RELEASE_GUARD)" run ${mvn} -B clean release:clean release:prepare -DpushChanges=false "$${NEXT_RELEASE[@]}"
	python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared "$$baseline"

_release _ci-release _release-dry-run _ci-release-dry-run:
	@if [[ -z "$${MAVEN_GPG_PASSPHRASE}" ]]; then
		echo "MAVEN_GPG_PASSPHRASE is empty, can't continue"
		exit 1
	fi
	if [[ "$${RELEASE_SKIP_TESTS}" == "true" ]] || [[ "$${RELEASE_SKIP_TESTS}" == "1" ]]; then
		export MAVEN_OPTS="$${MAVEN_OPTS} -DskipTests=true -DskipITs=true"
	fi
	python3 "$(RELEASE_GUARD)" run ${mvn} -B release:perform -Drelease.autopublish=$(if $(filter %-dry-run,$@),false,true)
