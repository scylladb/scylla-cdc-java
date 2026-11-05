const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {releaseTag, publish} = require('./github-release.cjs');
const commit = 'a'.repeat(40);
const tag = 'scylla-cdc-1.3.17';

test('reads only the prepared Maven release tag', () => {
  assert.equal(releaseTag(`# comment\nscm.tag=${tag}\n`), tag);
  for (const properties of ['', 'scm.tag=master', 'scm.tag=1.3.17',
    `scm.tag=${tag}\nscm.tag=${tag}`]) {
    assert.throws(() => releaseTag(properties));
  }
});

function fixture(existing = false) {
  const calls = [];
  const object = {type: 'commit', sha: commit};
  const github = {rest: {
    git: {
      getRef: async args => {
        calls.push(['tag', args]);
        return {data: {object: {type: 'tag', sha: 'b'.repeat(40)}}};
      },
      getTag: async () => ({data: {object}}),
    },
    repos: {
      getReleaseByTag: async () => {
        if (!existing) throw {status: 404};
        return {data: {body: 'Curated notes', tag_name: tag, draft: false}};
      },
      generateReleaseNotes: async args => {
        calls.push(['notes', args]);
        return {data: {body: 'Filtered notes'}};
      },
      createRelease: async args => calls.push(['create', args]),
    },
  }};
  return {github, context: {repo: {owner: 'scylladb', repo: 'scylla-cdc-java'}},
    core: {info: () => {}}, calls, object};
}

test('verifies remote annotated tag and publishes filtered generated notes', async () => {
  const f = fixture();
  await publish(f, tag, commit);
  assert.deepEqual(f.calls.map(call => call[0]), ['tag', 'notes', 'create']);
  assert.equal(f.calls[0][1].ref, `tags/${tag}`);
  assert.equal(f.calls[1][1].configuration_file_path, '.github/release.yml');
  assert.equal(f.calls[2][1].body, 'Filtered notes');
  assert.equal(f.calls[2][1].tag_name, tag);
});

test('reruns preserve existing curated releases', async () => {
  const f = fixture(true);
  await publish(f, tag, commit);
  assert.deepEqual(f.calls.map(call => call[0]), ['tag']);
});

test('missing or mismatched authoritative tags prevent release creation', async () => {
  const f = fixture();
  f.object.sha = 'c'.repeat(40);
  await assert.rejects(publish(f, tag, commit), /does not match/);
  f.github.rest.git.getRef = async () => {throw {status: 404};};
  await assert.rejects(publish(f, tag, commit));
  assert.equal(f.calls.some(call => call[0] === 'create'), false);
});

test('release lookup errors are not mistaken for absent releases', async () => {
  const f = fixture();
  f.github.rest.repos.getReleaseByTag = async () => {throw new Error('Forbidden');};
  await assert.rejects(publish(f, tag, commit), /Forbidden/);
  assert.equal(f.calls.some(call => call[0] === 'create'), false);
});

test('workflow retains trusted helper, publishes after SCM push, and skips dry runs', () => {
  const workflow = readFileSync('.github/workflows/release.yml', 'utf8');
  assert(workflow.indexOf('cp .github/scripts/github-release.cjs') <
    workflow.indexOf('- name: Checkout release target'));
  const publication = workflow.indexOf('- name: Create GitHub release from the published tag');
  assert(publication > workflow.indexOf('- name: Push changes to SCM'));
  assert(publication > workflow.indexOf('- name: Perform release'));
  assert.match(workflow.slice(publication), /if: inputs.dry-run == false/);
  assert.match(readFileSync('.github/release.yml', 'utf8'), /labels:\n      - omit-from-release-notes/);
});


test('existing draft or conflicting release is an actionable failure', async () => {
  for (const existing of [{tag_name: tag, draft: true},
    {tag_name: 'scylla-cdc-1.3.16', draft: false}]) {
    const f = fixture(true);
    f.github.rest.repos.getReleaseByTag = async () => ({data: existing});
    await assert.rejects(publish(f, tag, commit), /unpublished or.*conflicting/);
    assert.equal(f.calls.some(call => call[0] === 'create'), false);
  }
});
