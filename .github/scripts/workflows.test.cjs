// Copyright (C) 2026 ScyllaDB
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const yaml = require('js-yaml');

const workflow = yaml.load(readFileSync(join(__dirname, '../workflows/call_jira_sync.yml'), 'utf8'));
// These inspected conditions compare strings with lowercase literals. Actions
// ignores case in string comparisons; casefold these known fixture operands.
// This helper does not implement the general Actions expression language.
const routes = (action, label) => Object.entries(workflow.jobs).filter(([, job]) =>
  new Function('github', `return ${job.if}`)({event: {
    action: action.toLowerCase(), label: {name: label?.toLowerCase()}
  }})).map(([name]) => name);

test('Jira callers filter only exact omission-label add/remove events', () => {
  for (const [action, job] of [['labeled', 'jira-sync-add-label'], ['unlabeled', 'jira-sync-remove-label']]) {
    for (const label of ['omit-from-release-notes', 'Omit-from-release-notes', 'OMIT-FROM-RELEASE-NOTES']) {
      assert.deepEqual(routes(action, label), []);
    }
    for (const label of ['bug', 'omit-from-release-notes-extra', 'OMIT-FROM-RELEASE-NOTES-extra']) {
      assert.deepEqual(routes(action, label), [job]);
    }
  }
});

test('other Jira events retain their routes and reusable workflow authentication', () => {
  for (const [action, job] of [['opened', 'jira-sync-pr-opened'], ['edited', 'jira-sync-pr-opened'],
    ['ready_for_review', 'jira-sync-in-review'], ['review_requested', 'jira-sync-in-review'],
    ['closed', 'jira-sync-pr-closed']]) {
    assert.deepEqual(routes(action), [job]);
  }
  for (const job of Object.values(workflow.jobs)) {
    assert.match(job.uses, /^scylladb\/github-automation\/\.github\/workflows\/main_jira_sync_.*\.yml@main$/);
    assert.equal(job.secrets.caller_jira_auth, '${{ secrets.USER_AND_KEY_FOR_JIRA_AUTOMATION }}');
  }
});

test('test reports use check permissions and annotate fork pull requests without writing checks', () => {
  const tests = yaml.load(readFileSync(join(__dirname, '../workflows/tests.yml'), 'utf8'));
  assert.deepEqual(tests.permissions, {contents: 'read', checks: 'write'});
  for (const job of Object.values(tests.jobs)) {
    const reports = job.steps.filter(step => step.uses?.startsWith('mikepenz/action-junit-report@'));
    assert.equal(reports.length, 1);
    const report = reports[0];
    assert.equal(report.if, 'always()');
    const expression = report.with.annotate_only;
    assert.match(expression, /^\$\{\{ .* \}\}$/);
    const annotateOnly = new Function('github', `return ${expression.slice(3, -2)}`);
    for (const event of [
      {event_name: 'push', event: {}, expected: false},
      {event_name: 'pull_request', event: {pull_request: {head: {repo: {
        full_name: 'scylladb/scylla-cdc-java'
      }}}}, expected: false},
      {event_name: 'pull_request', event: {pull_request: {head: {repo: {
        full_name: 'contributor/scylla-cdc-java'
      }}}}, expected: true}
    ]) {
      assert.equal(annotateOnly({repository: 'scylladb/scylla-cdc-java', ...event}), event.expected);
    }
  }
});
