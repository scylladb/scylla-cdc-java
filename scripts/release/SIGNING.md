# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `FF45B448B57DFA0A43CD1E3C875E93451534DD48`
* Signing fingerprint: `79222D636C67BCEA99DDCBDCE5AEA706F2A39824`
* Identity: `ScyllaDB Publisher (scylladb/scylla-cdc-java) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:46:36+00:00`

The workflow first runs a `validate` job on its own runner without production
signing or package-publishing credentials. It checks the release tooling and
target policy, then builds the release profile with signing disabled. Project
tests run here unless `skip-tests` is selected; release helper tests always run.
Validation records the exact target commit as a job output.

The `release` job depends on successful validation and uses a fresh runner. Only
this job uses the GitHub deployment environment `release`, so approval is requested
after validation finishes. It checks out the validated source commit and independently
checks target policy before importing keys. Releases targeting `master` reject it
if it has advanced, including a final check immediately before publication.
It does not restore the validation workspace or Maven cache. Signed preparation,
signature verification, and publication happen after approval; project tests are
not repeated with production credentials. Releases must be dispatched from `master`.
The `target-tag` input selects `master` by default, or the commit preceding an
existing release tag. Historical sources must pass the same release configuration
checks; other source branches are unsupported.

Before running it, a repository owner must create and protect that environment,
configure its protection rules and allowed deployment refs, and store
`GPG_PRIVATE_KEY`
(ASCII-armored protected signing-subkey export) and `GPG_PASSPHRASE` (single-line
passphrase) as environment secrets. Migrate any repository-level copies to this
environment and remove those copies after validating the migration. The workflow
binding does not configure protection or migrate secrets; owner setup remains
required, including before dry runs and reruns.
Never upload the full primary private-key backup to CI. Packages, release commits,
and annotated release tags use the same signing subkey, selected by its complete
fingerprint with `!`. The primary key certifies the signing subkey and remains offline.

Locally, the `release-prepare`, `release`, and `release-dry-run` Make targets
require the active `gh` account to be `scylladb-publisher` before importing
credentials or changing the checkout; switch accounts with `gh auth switch` if
needed. Local dry runs and the underscored `_release*` recipes enforce the same
account requirement, even when `GITHUB_ACTIONS=true` is set locally.

The workflow invokes the shared Make targets `ci-release-prepare`, `ci-release`,
and `ci-release-dry-run`. Make owns the Maven commands, account checks, version
validation, preflight checks and signature-verification order. Local targets and
legacy underscored aliases use the same recipes without recursive Make calls.
Local targets authenticate `scylladb-publisher`; CI targets require
`GITHUB_ACTIONS=true` to catch accidental use outside Actions. The protected
`release` environment authorizes CI operators. The marker alone authenticates
nobody and grants no credentials. The triggering actor need not be the publisher.

Each recipe calls `signing.py maven PHASE COMMAND...` once. This low-level helper
manages only the temporary signing session, clean Maven environment, private
output capture and the single child process group. It forwards cancellation
before removing keys. `logs.py` owns the final scan and report. Direct low-level
helper calls rely on their caller for the account and phase-order policies.
It imports only the protected signing-subkey export, checks that primary
private material is absent, and verifies a fresh-agent signing preflight against
the pinned public certificate. Anonymous pipes supply the key import and passphrase;
credentials are not passed as command-line values or staged in separate input files.
Maven output can contain credentials and is captured privately as described below. Runtime Git settings apply only to the command and its children,
including Maven's local checkout clones. Sessions leave checkout Git configuration
unchanged and remove their temporary keyrings on success or failure. No credentials
or signing-session state are written to `GITHUB_ENV` or retained between workflow
steps. The workflow records the public pre-preparation commit SHA there as
`RELEASE_BASE_COMMIT` for later verification.

Maven's forked builds inherit the complete signing fingerprint with `!` through
`MAVEN_OPTS`; the GPG plugin receives `MAVEN_GPG_PASSPHRASE` through its environment.
Every GPG invocation reads the current passphrase, so checks and builds can outlive
the agent's passphrase cache. The protected signing-subkey export is consumed by
the session and removed from the child environment. Maven release builds need
access to the session keyring and passphrase while signing artifacts.

For maintainer-driven release preparation:

```sh
export RELEASE_GPG_PRIVATE_KEY_FILE=/private/path/signing-private.asc
export RELEASE_GPG_PASSPHRASE_FILE=/private/path/passphrase.txt
export RELEASE_VERSION=X.Y.Z
export RELEASE_BASE_COMMIT=$(git rev-parse HEAD)
make release-prepare
# Run additional checks against the prepared release.
make release-dry-run
# Use make release when ready for automatic publication.
```

Maven owns version changes, release and development commits, annotated tags, and
artifact signing. Maven stdout and stderr go directly to private files for the final scan.
Preparation verifies every new commit and the release tag against the pinned
certificate after closing its session, using public material only. The workflow repeats verification with
public material only. Perform and dry-run commands verify prepared history before
opening a new signing session. Keep `RELEASE_BASE_COMMIT` set to the original
pre-preparation commit for those commands; verification fails without that trusted
boundary. Do not derive it from the release tag's parent or replace it after
preparation. Do not change or replace existing release tags. A failed verification
blocks publication.

Both jobs check out the workflow launch SHA and preserve the Makefile, release helpers,
Maven installer and public certificate in runner temporary storage. Validation
then selects the requested source and records its exact SHA. The release job
uses that SHA even if the selected tag moves. It invokes the preserved Makefile through `make -f` and passes explicit paths to
the preserved helpers; historical Makefiles and helpers cannot replace them.
Historical releases never push prepared history or replace remote tags. Only a
release targeting `master` pushes its prepared history.

The supported Maven configuration is deliberately small. Make constructs
fixed commands from `RELEASE_SKIP_TESTS` (true/false or 1/0), `RELEASE_VERSION`
(letters, digits, dots, underscores, plus and minus), and the selected phase.
The signing helper clears inherited `MAVEN_ARGS`, `MAVEN_OPTS`, `MAVEN_CONFIG`, JVM option variables,
Maven base-directory overrides, recursive Make overrides, and SCM tokens. Startup
`mavenrc` files are disabled. Only generated test-skip and pinned
signing-key options enter Maven's environment. Validation also removes signing
and publishing credentials; preparation removes publishing credentials.
Nonempty `.mvn/maven.config` and `.mvn/jvm.config` files, including those in the
nearest ancestor `.mvn` directory, are unsupported and fail before importing keys.
Maintainers must review POMs and plugins as executable
build configuration; the guard does not emulate Maven interpolation or argument
parsing. It checks reactor path containment, GPG best-practice settings and the
minimum Central publishing plugin version. Ordinary Maven metadata expressions
and property aliases no longer need a custom parser's approval.

Complex checks can run before preparation or against signed outputs after a session
closes. Checks that need additional signatures can use their own scoped invocation:

```sh
python3 scripts/release/signing.py exec make YOUR_CHECK_TARGET
```

Use `python3 scripts/release/signing.py sign-files PATH...` to create and verify
detached artifact signatures. Missing signatures open a protected signing session;
existing signatures are preserved and verified with the pinned public certificate,
without requiring private credentials. Tampered files or signatures fail verification.

The low-level helper does not impose release-account policy. Wrapped commands have
no overall time limit; individual GPG operations have their own timeout. The workflow
uses the existing Actions job limit without a separate publication-step deadline.
Operators must interrupt hung local commands and check for partial publication
before retrying.

## Maven logs and final reporting

Maven runs in the foreground with stdout and stderr redirected to files with mode
0600 in a private directory with mode 0700. Raw output is never echoed to the
Actions console or included in an artifact upload. The signing helper has one process
supervisor and does not inspect output while Maven runs. Release plugins must
finish publication before their Maven command exits; detached background
publication is unsupported. There is no output-size rule that interrupts Maven.
Raw files consume runner disk space until the cycle ends; operators must provide
enough disk space for their builds.

In CI, `RELEASE_DEFER_LOGS=true` leaves output private until an `if: always()` report step
runs after publication, SCM push and any recovery-bundle preparation. Local Make
recipes use an EXIT trap to report automatically on success or failure, preserving
the phase result. Run the report manually if
a local process is killed before cleanup:

```sh
make release-report
```

`RELEASE_LOG_DIR` defaults locally to a directory under the system temporary
directory, specific to the current user and checkout. Keep any explicit override
outside the source tree so Maven clean and SCM preparation cannot remove or commit
it. The sibling directory with a `-private` suffix is reserved exclusively for raw
captures. Never upload that directory. Each report reads at most 64 MiB across completed command
logs. An invocation exceeding the remaining scan budget, interrupted output, or
a possible credential match causes both streams of that invocation to be withheld.
The scanner uses fixed-string searches for configured credentials and common
encoded forms, plus a small pattern for credential assignments and authorization
headers. It does not interpret Maven syntax. Detection is conservative and does
not guarantee discovery of arbitrary transformations or unknown credentials.

Only cleared streams or fixed withholding messages enter `stdout.log` and
`stderr.log`. `log-report.txt` records the result without matched values; CI also
adds warnings and a job summary. Raw captures are deleted after processing,
including when scanning fails. Abrupt runner loss or SIGKILL can prevent cleanup;
local operators must remove the private directory after recovery.

Maven and signature-verification failures still stop the release. Log findings or
reporting failures do not change a completed Maven result, interrupt publication,
or prevent a successful release's SCM push. A log warning alone is never a reason
to repeat publication. Check the Maven, verification and SCM step outcomes and
Central deployment state before retrying. Helper diagnostics withhold command
arguments, captured output and unexpected exception values. Run the release tests
with `python3 -m unittest discover -s scripts/release -p 'test_*.py' -v`.

Consumers should obtain this public certificate from this repository at a reviewed
commit, check the certification fingerprint above through a trusted channel, and
verify release signatures. The Ubuntu keyserver can also serve the certificate:

```sh
gpg --keyserver hkps://keyserver.ubuntu.com --recv-keys FF45B448B57DFA0A43CD1E3C875E93451534DD48
gpg --fingerprint FF45B448B57DFA0A43CD1E3C875E93451534DD48
gpg --status-fd 1 --verify ARTIFACT.asc ARTIFACT
```

Require exactly one `GOODSIG` and one `VALIDSIG` record. The `GOODSIG` key ID must
match the signing fingerprint's final 16 hexadecimal digits, and `VALIDSIG` must
contain the complete signing fingerprint above and the expected primary
fingerprint. Reject `EXPKEYSIG`, `REVKEYSIG`, `EXPSIG`, `BADSIG`, or `ERRSIG`, even
when a matching `VALIDSIG` is also present. `git verify-tag --raw TAG` and
`git verify-commit --raw COMMIT` provide the equivalent check for Git objects.
Keyserver presence alone establishes neither trust nor the expected repository
identity.

Rotate by preparing a new independent certificate, validating its protected subkey,
registering its public certificate on the publisher account, publishing the public
certificate, reviewing fingerprint/public-certificate updates together, and updating
the secret pair in a coordinated release pause. Keep old public certificates available
for historical releases. Never revoke the previous key merely to rotate it. If a key
is compromised, stop releases and use the offline revocation certificate; fix the
exposure before resuming. Do not rerun publication after a partial release without
following the repository's recovery instructions.

## Environment rollout

Create `release` with the Drivers team as required reviewer, admin bypass disabled,
self-approval permitted, and a custom deployment branch policy allowing only
`master` (no tags). This protection also covers dry runs and reruns. Dispatch from
`master`. Store these four secrets in the environment:

| Secret | Initial source/value |
| --- | --- |
| `GPG_PRIVATE_KEY` | `~/.repos-gpg/scylladb/scylla-cdc-java/signing-private.asc` |
| `GPG_PASSPHRASE` | `~/.repos-gpg/scylladb/scylla-cdc-java/passphrase.txt` |
| `SONATYPE_TOKEN_USERNAME` | `REPLACE_WITH_SONATYPE_TOKEN_USERNAME` |
| `SONATYPE_TOKEN_PASSWORD` | `REPLACE_WITH_SONATYPE_TOKEN_PASSWORD` |

Environment placeholders intentionally override repository secrets. The release
job names missing/placeholder Sonatype configuration and stops before signing
preparation or publication. No Central upload belongs to the placeholder rollout.
Keep repository-level copies until real Sonatype tokens are installed in the
environment and an approved dry run succeeds. Then delete all four repository
copies and repeat the approved dry run to prove environment-only operation.
GitHub cannot return stored secret values; retrieve or issue tokens separately.

`dry-run` performs a Central staging upload with automatic publication disabled
and no SCM push. It is not an offline simulation. Check the staged coordinates,
classifiers and signatures, then delete the staging deployment in Central.

## Recovery and first release

Before the first real release, confirm Java 11 validation (including Docker tests),
Maven 3.9.16, publisher key registration and public certificate availability, the
protected environment rules, and all four environment secrets. Check that base,
driver3 and lib artifacts retain their main, sources and javadoc artifacts and
that printer, replicator and state-redis examples are excluded. Review every
prepared commit and annotated `scylla-cdc-<version>` tag signature against the exact
subkey above. Only then approve automatic publication.

If publication fails, inspect the sanitized logs and Central deployment state
before retrying. Preserve the prepared checkout, `release.properties`, original
`RELEASE_BASE_COMMIT`, signed commits and tag outside ephemeral runners when
recovery is needed. A workflow rerun creates a fresh checkout; it is not an
automatic continuation of a partial deployment. If Central has a validated
unpublished deployment, finish or remove that deployment deliberately rather
than uploading it twice. If any coordinates are already published, never replace
them or reuse the version: choose a new `release-version` and document the gap.
If publication succeeded but SCM push failed, recover and verify the exact signed
history and push it after reconciling remote `master`; do not publish again.
On an SCM push failure, download the workflow's `signed-release-recovery`
artifact within 30 days. It contains a Git bundle with the exact prepared HEAD
and annotated release tag, plus the original `baseline`, final `head` and
`release-tag` values. It excludes checkout Git configuration, raw Maven
properties, logs and signing credentials. Fetch the bundle into a recovery
checkout and check out its recorded `head`. Use the signing helper and matching
public certificate from the reviewed workflow launch revision to verify the
imported commits and tag before reconciling and pushing:

```sh
python3 /trusted/release-tooling/signing.py verify-release \
  "$(cat /recovery/release-tag)" "$(cat /recovery/baseline)"
```

This public-key-only command accepts the tag and baseline directly, without
requiring `release.properties` or private credentials.
This artifact supports SCM recovery; it is not a resumable Maven deployment.

Dispatch recovery runs from `master`. A historical `target-tag` selects the
commit preceding that tag; it does not push prepared history or replace the
remote tag. Integrate recovery changes through a reviewed branch into `master`,
then start a new release using a new version if the old coordinates already
exist. Preserve existing remote tags, repository coordinates and the tag format
when recovering.
