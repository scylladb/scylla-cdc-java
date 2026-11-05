function releaseTag(properties) {
  const matches = [...properties.matchAll(/^scm\.tag=(.+)\r?$/gm)];
  const tag = matches.length === 1 ? matches[0][1].trim() : '';
  if (!/^scylla-cdc-\d+\.\d+\.\d+(?:[-.][A-Za-z0-9.-]+)?$/.test(tag)) {
    throw new Error('Expected one scylla-cdc version tag in release.properties');
  }
  return tag;
}

async function publish({github, context, core}, tag, expectedCommit) {
  if (!/^scylla-cdc-\d+\.\d+\.\d+(?:[-.][A-Za-z0-9.-]+)?$/.test(tag) ||
      !/^[0-9a-f]{40}$/.test(expectedCommit)) {
    throw new Error('Invalid release tag or commit');
  }
  const repo = context.repo;
  let object = (await github.rest.git.getRef({...repo, ref: `tags/${tag}`})).data.object;
  for (let depth = 0; object.type === 'tag' && depth < 10; depth++) {
    object = (await github.rest.git.getTag({...repo, tag_sha: object.sha})).data.object;
  }
  if (object.type !== 'commit' || object.sha !== expectedCommit) {
    throw new Error('Published tag does not match the prepared release commit');
  }
  try {
    const existing = (await github.rest.repos.getReleaseByTag({...repo, tag})).data;
    if (existing.draft || existing.tag_name !== tag) {
      throw new Error('Existing release is unpublished or has a conflicting tag');
    }
    core.info(`Release ${tag} already exists; preserving its notes`);
    return;
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  const notes = (await github.rest.repos.generateReleaseNotes({...repo, tag_name: tag,
    configuration_file_path: '.github/release.yml'})).data;
  await github.rest.repos.createRelease({...repo, tag_name: tag,
    name: tag, body: notes.body, draft: false, prerelease: false});
}

module.exports = {releaseTag, publish};
