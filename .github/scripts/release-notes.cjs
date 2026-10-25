const LABEL = 'omit-from-release-notes';

// These paths hold documentation, tests, or CI configuration, not shipped driver code.
function supportPath(path) {
  if (typeof path !== 'string' || path.split('/').some(part => part === '..' || !part)) {
    return false;
  }
  return /^\.github\//.test(path) ||
    /^(scylla-cdc-(base|driver3|lib|printer|replicator)|examples\/scylla-cdc-state-redis)\/src\/test\//.test(path) ||
    ['README.md', 'CHANGELOG.md', '.gitignore', 'renovate.json',
      '.github/RELEASE_NOTES.md', 'scylla-cdc-printer/README.md',
      'scylla-cdc-replicator/README.md', 'scylla-cdc-kafka-connect/README.md',
      'scripts/release/test_guard.py'].includes(path);
}

function supportOnly(files, count) {
  return Number.isSafeInteger(count) && count > 0 && files.length === count &&
    new Set(files.map(file => file.filename)).size === count &&
    files.every(file => supportPath(file.filename) &&
      (file.status !== 'renamed' || supportPath(file.previous_filename)));
}

async function classify({github, context, core}, number) {
  const repo = context.repo;
  const issue = {...repo, issue_number: number};
  async function include() {
    try {
      await github.rest.issues.removeLabel({...issue, name: LABEL});
    } catch (error) {
      if (error.status !== 404) throw error;
    }
  }
  let before;
  let files;
  let after;
  try {
    before = (await github.rest.pulls.get({...repo, pull_number: number})).data;
    files = await github.paginate(github.rest.pulls.listFiles,
      {...repo, pull_number: number, per_page: 100});
    after = (await github.rest.pulls.get({...repo, pull_number: number})).data;
    if (before.head.sha !== after.head.sha || before.base.sha !== after.base.sha ||
        before.changed_files !== after.changed_files || files.length !== after.changed_files ||
        new Set(files.map(file => file.filename)).size !== after.changed_files) {
      throw new Error('PR changed during classification or file listing is incomplete');
    }
  } catch (error) {
    try { await include(); } catch (cleanupError) {
      core.warning(`Could not remove stale omission label: ${cleanupError.message}`);
    }
    throw error;
  }
  if (!supportOnly(files, after.changed_files)) {
    await include();
    core.info('PR remains included in release notes');
    return;
  }
  try {
    try {
      await github.rest.issues.createLabel({...repo, name: LABEL, color: 'ededed',
        description: 'Documentation, tests, or CI only; omit from generated release notes'});
    } catch (error) {
      if (error.status !== 422) throw error;
    }
    await github.rest.issues.addLabels({...issue, labels: [LABEL]});
  } catch (error) {
    try { await include(); } catch (cleanupError) {
      core.warning(`Could not remove stale omission label: ${cleanupError.message}`);
    }
    throw error;
  }
  core.info('Support-only PR omitted from generated release notes');
}

module.exports = {supportPath, supportOnly, classify};
