// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const yaml = require('js-yaml');

const load = path => yaml.load(readFileSync(join(__dirname, '..', path), 'utf8'));

test('release-note labeling uses only the stock action and trusted configuration', () => {
  const workflow = load('workflows/label-release-notes.yml');
  assert.deepEqual(Object.keys(workflow.on), ['pull_request_target', 'workflow_dispatch']);
  assert.equal(workflow.on.workflow_dispatch.inputs['pull-request'].type, 'number');
  const job = workflow.jobs.classify;
  assert.equal(job.if, "github.event_name != 'workflow_dispatch' || github.ref == format('refs/heads/{0}', github.event.repository.default_branch)");
  assert.equal(job.steps.length, 1);
  assert.match(job.steps[0].uses, /^actions\/labeler@[0-9a-f]{40}$/);
  assert.deepEqual(job.steps[0].with, {
    'sync-labels': true,
    'pr-number': '${{ github.event.pull_request.number || inputs.pull-request }}'
  });
  assert.deepEqual(job.permissions,
    {contents: 'read', issues: 'write', 'pull-requests': 'write'});
  const policy = load('labeler.yml');
  assert.deepEqual(Object.keys(policy), ['omit-from-release-notes']);
  assert.deepEqual(Object.keys(policy['omit-from-release-notes'][0]['changed-files'][0]),
    ['all-globs-to-all-files']);
});
