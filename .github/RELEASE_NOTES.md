# GitHub release notes

The Maven release workflow creates a GitHub release after successful publication
and the SCM push, using the prepared `scylla-cdc-X.Y.Z` tag. It verifies that the
remote tag resolves to the prepared commit and uses GitHub-generated notes with
`.github/release.yml`. Dry runs do not create releases. Reruns preserve an existing
published release and its curated notes; drafts, conflicting tags, and other API
failures fail the workflow.

The `omit-from-release-notes` PR label excludes a PR from generated notes.
`actions/labeler` maintains it from `.github/labeler.yml` when all reported changed
filenames match the union of known documentation, test, or CI/development paths.
It runs on open, synchronization, reopen, edit, and close/merge, with
`sync-labels: true` removing the label when the filename rules no longer match.
Runs for a PR are serialized, including backfills.

Allowed paths are `.github/`, known module `src/test/` trees (including the Redis
example), the root README and changelog, module READMEs, `.gitignore`, Renovate
configuration, and the release guard's test file. Production sources and resources,
POMs, executable examples, release helpers, and unknown paths do not match.
Support-only mixtures qualify through one brace-union glob that must match every
reported changed filename. Author, title, and other labels do not affect matching.

Before a release, backfill already merged PRs through Actions → **Classify release
notes** → **Run workflow**, selecting the default branch and entering a PR number.
For example:

```sh
gh workflow run label-release-notes.yml --ref master -f pull-request=123
```

Run this once for each relevant merged PR and wait for successful labeling
before releasing. Review failed runs before generating notes. The stock action
fetches its repository configuration by API; the workflow does not check out PR
code. The label is managed automatically, so a later run can remove a manually
applied label when the filename rules do not match.

Release helper checks run with
`node --test .github/scripts/github-release.test.cjs` without publishing packages,
tags, or releases.
