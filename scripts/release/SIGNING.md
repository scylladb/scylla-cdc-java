# Repository release signing

The public certificate in `publisher.asc` is safe to commit and is registered on
`scylladb-publisher`. Keep it available to consumers; it does not belong in secrets.

* Certification fingerprint: `FF45B448B57DFA0A43CD1E3C875E93451534DD48`
* Signing fingerprint: `79222D636C67BCEA99DDCBDCE5AEA706F2A39824`
* Identity: `ScyllaDB Publisher (scylladb/scylla-cdc-java) <publish.code@scylladb.com>`
* Expiration: `2028-10-19T15:46:36+00:00`

Both manually dispatched workflows, `release.yml` and `release-dry-run.yml`, first run a `validate` job on its own runner without production
signing or package-publishing credentials. It checks the release tooling and
target policy, then builds the release profile with signing disabled. Project
tests always run for dry runs. Publishing alone accepts `skip-tests`; release helper tests always run.
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
existing release tag. Historical tags and their selected predecessors must belong to the workflow's
launch commit history on master, and sources must pass the same credential safety
checks; other source branches are unsupported. Both workflows accept `target-tag`
(default `master`) and optional `release-version=X.Y.Z`. A historical override must
match the requested `scylla-cdc-X.Y.Z` tag. Without an override, historical
preparation derives the version from that tag and verifies the prepared tag before
publication. Invalid inputs fail before building.

Both credentialed jobs share concurrency group `cdc-release`, with cancellation
disabled and [`queue: max`](https://github.blog/changelog/2026-05-07-github-actions-concurrency-groups-now-allow-larger-queues/)
retaining up to 100 pending jobs. Validation runs independently; queued or
approval-delayed jobs recheck `master` before signing and immediately before
publication. All pages of open
`release-blocker` issues and PRs are checked before validation, before approval,
after approval and before publication. API failures block progress.

```sh
# Signed staging upload, with mandatory project tests and environment approval:
gh workflow run release-dry-run.yml --ref master -f target-tag=master -f release-version=X.Y.Z
# Production publication (requires separate approval):
gh workflow run release.yml --ref master -f target-tag=master -f release-version=X.Y.Z
```

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

The protected workflow uses explicit `ci-release-prepare`, `ci-release`, and
`ci-release-dry-run` entrypoints, sharing the local phase recipes through their
`_ci-release*` inner targets. In Actions, the protected `release` environment
authorizes operators, including dry runs and reruns. The triggering actor need not
be the publisher account: signing still uses the repository's pinned publisher
subkey and identity. CI targets check `GITHUB_ACTIONS=true` to catch accidental use
outside Actions; that marker does not authenticate a caller or grant credentials.
Like the low-level signing helper, these explicit CI interfaces rely on the caller
to enforce the protected workflow boundary.

Each local public target and CI wrapper runs Maven in a scoped `signing.py exec`
session. It imports only the protected signing-subkey export, checks that primary
private material is absent, and verifies a fresh-agent signing preflight against
the pinned public certificate. Anonymous pipes supply the key import and passphrase;
neither credential is written to a temporary file or included in command-line
arguments. Runtime Git settings apply only to the command and its children,
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
export SONATYPE_TOKEN_USERNAME=YOUR_REAL_TOKEN_USERNAME
export SONATYPE_TOKEN_PASSWORD=YOUR_REAL_TOKEN_PASSWORD
export RELEASE_BASE_COMMIT=$(git rev-parse HEAD)
make release-prepare
# Run additional checks against the prepared release.
make release-dry-run
# For production, use make release INSTEAD of make release-dry-run.
```

Each preparation supports one perform command: successful `release:perform`
removes its Maven descriptor. After inspecting a dry run, remove its unpublished
Central deployment. Start with a fresh checkout and run `make release-prepare`
again before a later production release; do not run both perform commands on
the same preparation.

Maven owns version changes, release and development commits, annotated tags, and
artifact signing. Central publishing plugin `0.12.0` and release plugin `3.3.1`
retain Java 11, reactor updates, Maven coordinates and publication exclusions.
With the current `master` POM, publication waits for Central's `published`
state; dry runs wait for `validated` with automatic publication disabled.
Historical recovery uses the selected POM's publishing configuration and may
return at `validated`. Historical runs never push Git history or create GitHub
releases. The Make recipes reject missing
and placeholder Sonatype credentials before preparation and retain sanitized Maven stdout and stderr.
With the current POM, production permits up to 7200 seconds for Central's
published state; dry runs permit 1800 seconds for validation. A timeout still
requires inspecting Central before any retry: automatic publication may finish
after the client has stopped waiting. See the [Central plugin wait settings](https://central.sonatype.org/publish/publish-portal-maven/).
Preparation verifies every new commit and the release tag against the pinned
certificate before closing its session. The workflow repeats verification with
public material only. Perform and dry-run commands verify prepared history before
opening a new signing session. Keep `RELEASE_BASE_COMMIT` set to the original
pre-preparation commit for those commands; verification fails without that trusted
boundary. Do not derive it from the release tag's parent or replace it after
preparation. Do not change or replace existing release tags. A failed verification
blocks publication.

Both jobs first check out the workflow's launch SHA and preserve its Makefile,
release helpers and public certificate in runner temporary storage. Validation
then selects the requested source and records its exact SHA. The release job
uses that SHA even if the selected tag moves after validation. It executes the
preserved tooling through explicit paths, so historical Makefiles and helpers
cannot replace it. POMs and Maven runtime options from the selected source must
pass the trusted credential safety checks before any signing key is imported.
Shell commands, Make recipes and workflow structure are reviewed and tested with
the repository; the runtime guard does not parse them. Historical releases never
push their prepared commits or tags to SCM, and existing remote tags remain
unchanged. Only a release targeting `master` pushes its prepared history.

These checks inspect expressions without evaluating the Maven model. Simple
nonreserved aliases and canonical public metadata such as `${project.version}`
and `${project.build.directory}` are supported. Ambiguous Maven context-prefix
names such as `${projectLabel}` and `${projectAlias.version}` are conservatively
rejected even when a build could resolve a public value; use a nonreserved alias
or canonical metadata path instead.

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

Log capture retains at most 64 MiB of combined stdout and stderr. Exceeding this
limit discards both streams and records a fixed overflow message, even if only
one stream was noisy. Maven continues running and its exit status is preserved;
log overflow does not interrupt publication. Successful Maven parents with
pipe-connected descendants wait for their output streams to close before the
workflow can push release history. A collection error before EOF fails the command
even if the parent exited successfully. Failed parents have a bounded drain period,
then remaining descendants are stopped before returning failure. Signing commands
also stop their remaining process group before removing the session keyring.

A failed command propagates its exit status and still cleans up its session. Helper errors
identify a fixed operation and status or category without printing command arguments,
captured output, or exception payloads. Maven signs release artifacts; the helper's
`verify-prepared` command verifies the prepared commits and tag using public
material only. Run `make test-release-signing` to test with disposable keys.

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
| `GPG_PRIVATE_KEY` | Protected signing-subkey export for this repository |
| `GPG_PASSPHRASE` | Single-line passphrase for that subkey |
| `SONATYPE_TOKEN_USERNAME` | `REPLACE_WITH_SONATYPE_TOKEN_USERNAME` |
| `SONATYPE_TOKEN_PASSWORD` | `REPLACE_WITH_SONATYPE_TOKEN_PASSWORD` |

Environment placeholders intentionally override repository secrets. The release
job names missing/placeholder Sonatype configuration and stops before signing
preparation or publication. No Central upload belongs to the placeholder rollout.
Keep repository-level copies until real Sonatype tokens are installed in the
environment and an approved dry run succeeds. Then delete all four repository
copies and repeat the approved dry run to prove environment-only operation.
GitHub cannot return stored secret values; retrieve or issue tokens separately.

The separate `release-dry-run.yml` workflow (or local `make release-dry-run`) performs a Central staging upload with automatic publication disabled
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
before retrying. Download the recovery artifacts described below and preserve
the original `RELEASE_BASE_COMMIT`, signed commits and tag. A workflow rerun creates a fresh checkout; it is not an
automatic continuation of a partial deployment. If Central has a validated
unpublished deployment, finish or remove that deployment deliberately rather
than uploading it twice. If any coordinates are already published, never replace
them or reuse the version: choose a new `release-version` and document the gap.
If publication succeeded but SCM push failed, recover and verify the exact signed
history and push it after reconciling remote `master`; do not publish again.

Historical recovery selects the requested tag's predecessor and reruns Maven
preparation and publication only when its POMs and runtime configuration satisfy
the trusted credential policy. Old tags without those controls are rejected.
Historical runs never push Git history or create a GitHub Release. If historical
coordinates already exist, use a new version on `master`; never replace artifacts
or remote tags.

## GitHub distribution and partial publication

After Central publication and the successful master push, the workflow signs and
verifies four binaries from Maven's `target/checkout`, using the prepared metadata
and POMs in the signed tag rather than the next snapshot or newest tag:

- `scylla-cdc-base-X.Y.Z.jar`
- `scylla-cdc-lib-X.Y.Z.jar`
- `scylla-cdc-driver3-X.Y.Z.jar`
- `scylla-cdc-replicator-X.Y.Z-jar-with-dependencies.jar` (existing fat JAR)

Each has a detached `.asc` signature: eight GitHub assets total. Existing Maven
signatures are verified without overwriting; the replicator's distribution
signature does not change its Maven publication exclusion. `target/release-prepared.properties`
retains the public Maven identity because successful `release:perform` removes
`release.properties`.

`make github-release` requires `scylladb-publisher`; `ci-github-release` is the
protected workflow entrypoint. The helper verifies prepared history and all
signatures with public material, requires the exact signed tag object on GitHub,
and refuses every existing draft or release. It generates filtered notes using
`.github/release.yml`, creates a draft with all eight assets, checks paginated
asset metadata, downloads every uploaded file and compares SHA-256 with the
verified local files, then rechecks the tag and draft before publishing. API,
upload, signature or completeness failures leave publication stopped. Draft
discovery and downloads allow three read attempts with one-second pauses for
GitHub visibility delays; creation and publication writes are never retried. Existing
assets and curated notes are never replaced on reruns. No Confluent publication
or notification is part of this flow.

After successful preparation, both workflows upload `release-recovery-ATTEMPT`
before starting Central publication. It contains an incremental Git bundle of
the verified signed commits and annotated tag, `recovery.json` with their exact
IDs and original baseline, and allowlisted public Maven release identity.
Other Maven descriptor fields and release credentials are excluded. A failed
bundle or upload blocks publication. Existing recovery directories are never
overwritten.

A later `always()` step attempts to retain built JARs and available `.asc` files
in `release-built-artifacts-ATTEMPT`, preserving the Maven module layout. This
includes sources and javadoc JARs when present; it does not change the eight
public GitHub Release assets. Both archives have seven-day retention, with a
separate name for each workflow attempt. Download them promptly. A runner lost
before the final upload may lack the binary archive, but the signed history was
already uploaded before Central started. Sanitized logs do not contain the
prepared workspace. A workflow rerun starts a new preparation; it cannot resume
a partial release automatically.

Recover in a fresh clone containing the original baseline. Set `RECOVERY_DIR`
to an absolute directory outside that clone and obtain `RECOVERY_TAG` from
`recovery.json`. Confirm its baseline against the original validated source;
keep that trusted value in `RELEASE_BASE_COMMIT`. Replace `ATTEMPT` below with
the workflow's `github.run_attempt` number, usually `1`. Preserve the trusted
verification helper and certificate outside the clone before changing source.

```sh
export RECOVERY_TOOLING=$(mktemp -d)
cp scripts/release/signing.py scripts/release/publisher.asc "$RECOVERY_TOOLING/"
export RELEASE_SIGNING_HELPER="$RECOVERY_TOOLING/signing.py"
gh run download RUN_ID --name release-recovery-ATTEMPT --dir "$RECOVERY_DIR"
git bundle verify "$RECOVERY_DIR/release.bundle"
git fetch "$RECOVERY_DIR/release.bundle" \
  HEAD:refs/heads/release-recovery \
  "refs/tags/$RECOVERY_TAG:refs/tags/$RECOVERY_TAG"
git switch release-recovery
mkdir -p target
cp "$RECOVERY_DIR/release-prepared.properties" target/release-prepared.properties
python3 "$RELEASE_SIGNING_HELPER" verify-prepared
gh run download RUN_ID --name release-built-artifacts-ATTEMPT --dir target/checkout
```

Use the trusted current release helpers and publisher certificate when the
recovered historical checkout does not contain them. Retain the trusted helper
paths before selecting historical source, as the workflows do. If the binary
archive is unavailable, reconstruct missing outputs from the exact signed tag
without deploying, and retrieve already-published Maven JARs and signatures
from Central before running the GitHub-only recovery. Never repeat a successful
Central upload just to rebuild missing GitHub assets.

1. Inspect Central and GitHub before any retry. If Central succeeded and Git push
   failed, recover the exact signed commits/tag, verify them against the original
   baseline, reconcile `master`, and push only that history. Do not upload again.
2. If Git succeeded but no GitHub release exists, restore that same trusted
   checkout and run public verification, then publish only the GitHub step:

   ```sh
   export RELEASE_BASE_COMMIT=ORIGINAL_PREPARATION_BASELINE
   python3 scripts/release/signing.py verify-prepared
   python3 scripts/release/assets.py identity
   # Only if an expected signature is missing, with the protected subkey available:
   python3 scripts/release/assets.py sign
   make github-release
   ```

3. If a draft exists, inspect it manually. Download its assets with
   `gh release download scylla-cdc-X.Y.Z --dir RECOVERY_DIRECTORY`, verify all four
   `.asc` signatures against the certificate above, compare all eight files to
   the original signed outputs, check the remote signed tag, and review the notes.
   Complete missing uploads deliberately, then publish that inspected draft with
   `gh release edit scylla-cdc-X.Y.Z --draft=false`. The automatic helper refuses
   drafts so it cannot silently overwrite a partially completed publication.
4. If the release is already published, preserve its assets and notes. A lost
   publication response requires read-only inspection, not another release run.

Before the next real release after merge, inspect the protected environment and
secret names, install real Central credentials, and complete an explicitly approved
staging dry run. Delete its unpublished Central deployment after inspection.
See [release-note classification and backfill](../../.github/RELEASE_NOTES.md)
for the required pre-release range checks.
