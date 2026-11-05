# Release notes

Normal master releases create a GitHub Release after successful Central publication
and the signed Git push. The helper verifies the four Java distribution binaries
and four signatures, stages them in a draft, verifies all uploaded bytes, and then
publishes. Existing releases and drafts fail without replacing notes or assets.
Historical runs and staging dry runs never create GitHub releases.
See [signing and recovery](../scripts/release/SIGNING.md) before retrying any step.

`.github/release.yml` excludes PRs labeled `omit-from-release-notes`. The label is
for GitHub release presentation. Local Jira label-event callers ignore its manual
addition/removal while preserving other Jira events. Existing Jira copies are not
cleaned; other synchronization paths can restore previously copied metadata.

## Automatic labeling

The workflow runs only `actions/labeler`, using `.github/labeler.yml` fetched from
the repository through the action's API. It runs on PR open, synchronize, reopen,
edit and close events, with manual dispatch for backfill. No PR code is checked out
or executed. Manual dispatch must select the repository default branch.

Every changed filename must match the configured support paths. Explicit README
and changelog paths, reactor `src/test/` trees, the listed ordinary CI workflows,
release helper tests, `.gitignore` and `renovate.json` qualify. Production code and
resources, POMs, `Makefile`, executable examples, release helpers, release workflows
and composite actions, release-note configuration, lockfiles and unknown paths
remain visible. Author, title, `chore` and `dependencies` labels do not determine
exclusion. `sync-labels: true` removes the omission label when files stop matching.

The stock action matches current filenames, including the destination of a rename.
Review renames manually before generating notes; a production file renamed into a
support path should remain visible. Failed labeling can leave an earlier label in
place, so inspect the current labels after rerunning the workflow.

## Before generating notes

1. Identify every PR since the previous release, including older merged PRs.
2. Backfill labels through Actions → Classify release notes → Run workflow, or:

   ```sh
   gh workflow run label-release-notes.yml --ref master -f pull-request=123
   ```

3. Wait for classification to succeed. Review each PR's changed files and current
   omission label, including both names of renamed files. Correct labels manually
   when needed; another classification run reapplies the filename policy.
4. Generate notes after this inspection. Label changes do not update notes that
   have already been published.
