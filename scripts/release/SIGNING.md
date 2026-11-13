# CDC releases

Releases run only through `.github/workflows/release.yml`, dispatched from
`master`. Local publication, historical source targeting and standalone artifact
signing are unsupported. Maven signs release artifacts during the release build.

## Release flow

1. `validate` checks the release configuration and helper tests, then builds the
   unsigned release profile without production credentials. Project tests run
   unless `skip-tests` is selected. It records the exact source commit.
2. A fresh `release` runner checks out that commit. The protected `release`
   environment requests approval only after validation succeeds. Before importing
   signing material, the job rejects a moved `master`, unsafe configuration and
   missing or placeholder publishing credentials.
3. `make ci-release-prepare` creates signed release/development commits and an
   annotated tag without pushing them. Public-key verification must succeed.
4. After another blocker and `master` check, `make ci-release` publishes, or
   `make ci-release-dry-run` stages artifacts with automatic publication disabled.
   A real release then pushes the prepared history to `master`.
5. An always-run step scans Maven logs after publication and SCM recovery steps.

Dry runs require the same protected approval and credentials. They upload to
Central staging, leave automatic publication disabled and do not push SCM changes.
Inspect and remove the staged deployment afterward. They are not offline previews.

The Makefile defines Maven commands, version validation and verification order.
CI targets require `GITHUB_ACTIONS=true` to catch accidental local use; this marker
is not authentication. Environment protection authorizes the operator. Validation
and public-key verification can run without private credentials.

## Signing and configuration

The public certificate in `publisher.asc` is safe to distribute and is registered
on `scylladb-publisher`:

- Certification fingerprint: `FF45B448B57DFA0A43CD1E3C875E93451534DD48`
- Signing fingerprint: `79222D636C67BCEA99DDCBDCE5AEA706F2A39824`
- Identity: `ScyllaDB Publisher (scylladb/scylla-cdc-java) <publish.code@scylladb.com>`
- Expiration: `2028-10-19T15:46:36+00:00`

Keep the primary private key offline. The signing helper imports only the protected
signing-subkey export into a temporary keyring, rejects primary private material,
and verifies a fresh-agent preflight against the pinned certificate. Anonymous
pipes supply import/passphrase inputs. Runtime Git configuration selects the exact
subkey with `!` for commits and tags without changing checkout configuration.
Maven receives the same signer through `MAVEN_OPTS` and its passphrase through
`MAVEN_GPG_PASSPHRASE`. The private export is removed from Maven's environment.
Cancellation stops Maven's process group before keyring and agent cleanup.

Only environment-secret inputs are supported: `RELEASE_GPG_PRIVATE_KEY` and
`MAVEN_GPG_PASSPHRASE`. No credential-file options or passphrase aliases are provided.
`RELEASE_BASE_COMMIT` records the original pre-preparation SHA. Verification checks
all subsequent commits and the annotated tag against the pinned signer. Do not
replace that baseline or derive it from the release tag's parent.

Make constructs fixed commands from `RELEASE_SKIP_TESTS`, `RELEASE_VERSION` and the
selected phase. The helper clears inherited Maven/JVM options, base-directory
overrides, Make overrides and SCM tokens. It disables `mavenrc` startup files.
Nonempty Maven/JVM option files, including the nearest ancestor `.mvn` configuration,
are unsupported. The guard checks reactor paths, GPG best-practice settings and
the minimum Central publishing plugin version. Maintainers review POMs and plugins
as executable build configuration; the guard does not emulate Maven parsing.
Java 11, Maven 3.9.16, reactor version updates, coordinates, classifiers and example
exclusions remain unchanged.

## Maven logs

`RELEASE_LOG_DIR` points outside the checkout to runner temporary storage. Maven
stdout and stderr go directly to files with mode 0600 under a directory with mode
0700. Raw output is never echoed or uploaded. Maven must finish publication before
exiting; detached background publication is unsupported. Commands have no extra
overall deadline. GPG operations retain their own timeout and Actions retains its
job limit. Raw capture uses runner disk space without interrupting publication at
an output-size threshold.

The final `make release-report` step scans at most 64 MiB across completed logs.
It searches configured credentials and common encodings, plus credential-assignment
and authorization-header patterns. Possible matches, incomplete output or logs
exceeding the scan budget cause both streams of that invocation to be withheld.
Only cleared output or fixed diagnostics enter the uploaded logs. The scan is
conservative and cannot guarantee detection of unknown credentials or arbitrary
transformations. Raw files are removed after processing, including scan failures;
abrupt runner loss can prevent cleanup.

The report emits warnings, a job summary and `log-report.txt` without matched
values. Log findings never change a completed Maven result or prevent the SCM
push. Maven and signature failures still stop the release. A log warning alone is
not a reason to repeat publication; inspect the release outcome and Central state.

## Environment rollout

Create the `release` environment with the Drivers team as required reviewer, admin
bypass disabled, self-approval permitted and a deployment policy allowing only
`master`, with no tags. Store these environment secrets:

| Secret | Initial source/value |
| --- | --- |
| `GPG_PRIVATE_KEY` | `~/.repos-gpg/scylladb/scylla-cdc-java/signing-private.asc` |
| `GPG_PASSPHRASE` | `~/.repos-gpg/scylladb/scylla-cdc-java/passphrase.txt` |
| `SONATYPE_TOKEN_USERNAME` | `REPLACE_WITH_SONATYPE_TOKEN_USERNAME` |
| `SONATYPE_TOKEN_PASSWORD` | `REPLACE_WITH_SONATYPE_TOKEN_PASSWORD` |

Placeholders intentionally override repository secrets and fail before preparation
or publication. No Central upload belongs to this phase. Install real Sonatype
tokens separately, complete an approved dry run, then remove repository-level
copies of all four secrets and repeat the dry run. GitHub cannot return stored
secret values. Workflow binding alone does not provision environment protection.

## Recovery

Inspect Central before retrying any failed publication. An unpublished deployment
must be completed or removed deliberately. Published coordinates must never be
replaced or reused; select a new version when needed. A workflow rerun starts from
a fresh checkout and does not resume a partial deployment.

If publication succeeds but the SCM push fails, download `signed-release-recovery`
within 30 days. It contains the exact signed Git history bundle and recorded
`baseline`, `head` and `release-tag`, without checkout configuration, Maven
properties, logs or credentials. Import it into a recovery checkout, check out its
recorded head, and verify using the matching reviewed helper and public certificate:

```sh
python3 /trusted/release-tooling/signing.py verify-release \
  "$(cat /recovery/release-tag)" "$(cat /recovery/baseline)"
```

Reconcile remote `master` before pushing the recovered history. Do not publish
again. Integrate any further fixes into `master` and dispatch a new workflow.
Keep old public certificates available for verification after key rotation.

Run the release tests with:

```sh
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
```
