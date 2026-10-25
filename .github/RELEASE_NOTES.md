# GitHub release notes

The Maven release workflow creates a GitHub release after successful publication
and the SCM push, using the prepared `scylla-cdc-X.Y.Z` tag. It verifies that the
remote tag resolves to the prepared commit and uses GitHub-generated notes with
`.github/release.yml`. Dry runs do not create releases. Reruns preserve an existing
published release and its curated notes; drafts, conflicting tags, and other API
failures fail the workflow.

The `omit-from-release-notes` PR label excludes a PR from generated notes. The
classifier creates the label when needed and applies it only when every changed
path is known documentation, tests, or CI/development configuration. It reevaluates
PRs on open, synchronization, reopen, edit, and close/merge, removing stale labels
when production changes appear. Runs for a PR are serialized, including backfills.

Allowed paths are `.github/`, known module `src/test/` trees (including the Redis
example), the root README and changelog, module READMEs, `.gitignore`, Renovate
configuration, and the release guard's test file. Production sources and resources,
POMs, executable examples, release helpers, unknown paths, and renames from any of
these into support paths stay visible. Support-only mixtures qualify. Runtime
dependency changes remain visible regardless of author, title, or other labels.
The allowlist lives in `.github/scripts/release-notes.cjs`.

Before a release, backfill already merged PRs through Actions → **Classify release
notes** → **Run workflow**, selecting the default branch and entering a PR number.
For example:

```sh
gh workflow run label-release-notes.yml --ref master -f pull-request=123
```

Run this once for each relevant merged PR and wait for successful classification
before releasing. Incomplete or duplicate file lists, changing head/base snapshots,
and API failures fail visibly and attempt to remove stale omission labels. Review
failed runs before generating notes. The classifier uses trusted default-branch
code and never checks out a PR head with write permissions. Its label is managed
automatically, so a later classification can remove a manually applied label.

Helper checks run with `node --test .github/scripts/*.test.cjs` without publishing
packages, tags, or releases.
