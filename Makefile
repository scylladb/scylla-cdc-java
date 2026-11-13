SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c
.DEFAULT_GOAL := verify
RELEASE_GUARD ?= scripts/release/guard.py
RELEASE_SIGNING_HELPER ?= $(dir $(RELEASE_GUARD))signing.py
RELEASE_LOG_HELPER ?= $(dir $(RELEASE_GUARD))logs.py
RELEASE_SKIP_TESTS ?= false
RELEASE_VERSION ?=
RELEASE_DEFER_LOGS ?= false
export RELEASE_SKIP_TESTS RELEASE_VERSION RELEASE_DEFER_LOGS

.PHONY: verify test-release-signing release-check-user release-check-ci release-validate release-report
.PHONY: release-prepare release release-dry-run _release-prepare _release _release-dry-run
.PHONY: ci-release-prepare ci-release ci-release-dry-run _ci-release-prepare _ci-release _ci-release-dry-run

verify:
	mvn -ntp verify

release-check-user:
	@user=$$(gh api user --jq .login 2>/dev/null) || user=
	if [[ "$$user" != scylladb-publisher ]]; then
		echo "Release requires GitHub user scylladb-publisher; run gh auth login or gh auth switch." >&2
		exit 1
	fi

release-check-ci:
	@if [[ "$${GITHUB_ACTIONS:-}" != true ]]; then
		echo "Use local release targets outside GitHub Actions." >&2
		exit 1
	fi

release-prepare release release-dry-run _release-prepare _release _release-dry-run: release-check-user
ci-release-prepare ci-release ci-release-dry-run _ci-release-prepare _ci-release _ci-release-dry-run: release-check-ci

# Each recipe owns its final report. CI defers it until after SCM/recovery steps.
define release-start
if [[ "$${RELEASE_DEFER_LOGS}" != true ]]; then
	trap 'python3 "$(RELEASE_LOG_HELPER)" report || true' EXIT
fi
python3 "$(RELEASE_GUARD)" check
endef

release-validate:
	@$(release-start)
	python3 "$(RELEASE_SIGNING_HELPER)" maven validate mvn -B --no-transfer-progress -Prelease -Dgpg.skip=true verify

release-prepare _release-prepare ci-release-prepare _ci-release-prepare:
	@$(release-start)
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

release _release ci-release _ci-release release-dry-run _release-dry-run ci-release-dry-run _ci-release-dry-run:
	@$(release-start)
	python3 "$(RELEASE_SIGNING_HELPER)" verify-prepared
	python3 "$(RELEASE_SIGNING_HELPER)" maven $(if $(filter %-dry-run,$@),dry-run,perform) mvn -B --no-transfer-progress release:perform -Drelease.autopublish=$(if $(filter %-dry-run,$@),false,true)

release-report:
	python3 "$(RELEASE_LOG_HELPER)" report

test-release-signing:
	python3 -m unittest discover -s scripts/release -p 'test_signing.py' -v
