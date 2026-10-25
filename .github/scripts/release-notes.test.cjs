const {test} = require('node:test');
const assert = require('node:assert/strict');
const {supportOnly, classify} = require('./release-notes.cjs');
const file = filename => ({filename, status: 'modified'});

test('only known support paths qualify, including mixtures', () => {
  const files = ['CHANGELOG.md', 'scylla-cdc-base/src/test/java/QueryTest.java',
    '.github/workflows/tests@v1.yml', 'README.md'].map(file);
  assert.equal(supportOnly(files, files.length), true);
  for (const path of ['core/src/main/java/Query.java', 'pom.xml', 'core/pom.xml',
    'examples/pom.xml', 'scripts/release/signing.py', 'unknown.md', 'tools/build.sh']) {
    assert.equal(supportOnly([...files, file(path)], files.length + 1), false, path);
  }
  assert.equal(supportOnly([], 0), false);
  assert.equal(supportOnly(files, files.length + 1), false);
});

test('renames require both old and new support paths', () => {
  for (const [filename, previous_filename, expected] of [
    ['README.md', 'CHANGELOG.md', true],
    ['README.md', 'core/src/main/java/Query.java', false],
    ['core/src/main/java/Query.java', 'README.md', false],
    ['README.md', undefined, false],
  ]) {
    assert.equal(supportOnly([{filename, previous_filename, status: 'renamed'}], 1), expected);
  }
});

function fixture(files, count = files.length) {
  const calls = [];
  const pull = {head: {sha: 'head'}, base: {sha: 'base'}, changed_files: count};
  const github = {
    rest: {
      pulls: {
        get: async () => ({data: pull}),
        listFiles: () => {},
      },
      issues: {
        createLabel: async args => calls.push(['create', args]),
        addLabels: async args => calls.push(['add', args]),
        removeLabel: async args => calls.push(['remove', args]),
      },
    },
    paginate: async (endpoint, args) => {
      assert.equal(endpoint, github.rest.pulls.listFiles);
      assert.equal(args.per_page, 100);
      calls.push(['paginate']);
      return files;
    },
  };
  return {github, context: {repo: {owner: 'scylladb', repo: 'scylla-cdc-java'}},
    core: {info: () => {}, warning: () => {}}, calls, pull};
}

test('pagination can classify more than one page and create missing label', async () => {
  const f = fixture(Array.from({length: 201}, (_, i) => file(`scylla-cdc-lib/src/test/resources/page${i}.json`)));
  await classify(f, 23);
  assert.deepEqual(f.calls.map(call => call[0]), ['paginate', 'create', 'add']);
});

test('existing label is reused', async () => {
  const f = fixture([file('README.md')]);
  f.github.rest.issues.createLabel = async () => {throw {status: 422};};
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'add');
});

test('mixed PR removes stale omission label', async () => {
  const f = fixture([file('README.md'), file('pom.xml')]);
  await classify(f, 23);
  assert.equal(f.calls.at(-1)[0], 'remove');
  assert.equal(f.calls.at(-1)[1].name, 'omit-from-release-notes');
});

test('truncated listings and API failures remove stale label and fail visibly', async () => {
  for (const failure of ['truncated', 'api']) {
    const f = fixture([file('README.md')], 2);
    if (failure === 'api') f.github.paginate = async () => {throw new Error('API failure');};
    await assert.rejects(classify(f, 23));
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('concurrent PR updates are not omitted using stale file data', async () => {
  const f = fixture([file('README.md')]);
  let requests = 0;
  f.github.rest.pulls.get = async () => ({data: {...f.pull,
    head: {sha: ++requests === 1 ? 'old' : 'new'}}});
  await assert.rejects(classify(f, 23), /PR changed/);
  assert.equal(f.calls.at(-1)[0], 'remove');
});

test('missing stale label is harmless; permission failures propagate', async () => {
  const f = fixture([file('pom.xml')]);
  f.github.rest.issues.removeLabel = async () => {throw {status: 404};};
  await classify(f, 23);
  f.github.rest.issues.removeLabel = async () => {throw new Error('Forbidden');};
  await assert.rejects(classify(f, 23), /Forbidden/);
});


test('listing with duplicate filenames is rejected even when counts match', async () => {
  const f = fixture([file('README.md'), file('README.md')]);
  assert.equal(supportOnly([file('README.md'), file('README.md')], 2), false);
  await assert.rejects(classify(f, 23), /incomplete/);
  assert.equal(f.calls.at(-1)[0], 'remove');
});

test('base SHA changes and changed file count changes invalidate snapshot', async () => {
  for (const field of ['base', 'changed_files']) {
    const f = fixture([file('README.md')]);
    let requests = 0;
    f.github.rest.pulls.get = async () => ({data: {...f.pull,
      ...(++requests === 2 ? {[field]: field === 'base' ? {sha: 'new'} : 2} : {})}});
    await assert.rejects(classify(f, 23), /PR changed/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('label API errors remove stale label and fail visibly', async () => {
  for (const operation of ['createLabel', 'addLabels']) {
    const f = fixture([file('README.md')]);
    f.github.rest.issues[operation] = async () => {throw new Error('API failure');};
    await assert.rejects(classify(f, 23), /API failure/);
    assert.equal(f.calls.at(-1)[0], 'remove');
  }
});

test('cleanup failure retains original API error and warns', async () => {
  const f = fixture([file('README.md')]);
  const warnings = [];
  f.core.warning = message => warnings.push(message);
  f.github.paginate = async () => {throw new Error('Listing failed');};
  f.github.rest.issues.removeLabel = async () => {throw new Error('Permission denied');};
  await assert.rejects(classify(f, 23), /Listing failed/);
  assert.match(warnings[0], /Permission denied/);
});

test('known module tests and documentation qualify but executable examples do not', () => {
  for (const path of ['scylla-cdc-lib/src/test/resources/fixture.json',
    'examples/scylla-cdc-state-redis/src/test/java/RedisStateStoreIT.java',
    'scylla-cdc-printer/README.md', 'scripts/release/test_guard.py']) {
    assert.equal(supportOnly([file(path)], 1), true, path);
  }
  for (const path of ['scylla-cdc-driver3/pom.xml', 'renovate.md',
    'examples/scylla-cdc-state-redis/src/main/java/RedisStateStore.java',
    'scylla-cdc-printer/scylla-cdc-printer', 'Makefile', 'release.properties',
    'scylla-cdc-lib/src/main/resources/config.json', 'scripts/release/guard.py',
    'unknown/src/test/fixture.json', '../README.md']) {
    assert.equal(supportOnly([file(path)], 1), false, path);
  }
});
