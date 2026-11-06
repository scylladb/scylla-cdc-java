#!/usr/bin/env python3
"""Publish the signed Java CDC distribution using prepared Maven metadata."""

import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
import time
import xml.etree.ElementTree as ET

import signing

PREFIX = "scylla-cdc-"
MODULES = ("scylla-cdc-base", "scylla-cdc-lib", "scylla-cdc-driver3", "scylla-cdc-replicator")
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def identity():
    metadata = Path("release.properties")
    if not metadata.exists():
        metadata = signing.PREPARED_METADATA
    properties = {}
    for line in metadata.read_text().splitlines():
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        key = key.replace("\\:", ":")
        if separator:
            if key in properties:
                raise signing.SigningError("Duplicate prepared Maven metadata.")
            properties[key] = value
    tag = properties.get("scm.tag", "")
    if not re.fullmatch(PREFIX + r"[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise signing.SigningError("Expected a prepared scylla-cdc-X.Y.Z release tag.")
    version = tag[len(PREFIX):]
    if properties.get("project.rel.com.scylladb:scylla-cdc") != version:
        raise signing.SigningError("Prepared Maven version and release tag disagree.")
    pom = tagged_pom(tag, "pom.xml")
    if (pom.findtext("m:groupId", namespaces=NS) != "com.scylladb"
            or pom.findtext("m:artifactId", namespaces=NS) != "scylla-cdc"
            or pom.findtext("m:version", namespaces=NS) != version
            or not set(MODULES).issubset({e.text for e in pom.findall("m:modules/m:module", NS)})):
        raise signing.SigningError("Prepared tag does not contain the expected Maven reactor identity.")
    return tag, version


def tagged_pom(tag, path):
    return ET.fromstring(signing.run(["git", "show", f"refs/tags/{tag}:{path}"]))


def artifacts():
    tag, version = identity()
    paths = []
    for module in MODULES:
        pom = tagged_pom(tag, module + "/pom.xml")
        if (pom.findtext("m:artifactId", namespaces=NS) != module
                or pom.findtext("m:parent/m:groupId", namespaces=NS) != "com.scylladb"
                or pom.findtext("m:parent/m:artifactId", namespaces=NS) != "scylla-cdc"
                or pom.findtext("m:parent/m:version", namespaces=NS) != version
                or pom.findtext("m:version", default=version, namespaces=NS) != version
                or pom.findtext("m:packaging", default="jar", namespaces=NS) != "jar"):
            raise signing.SigningError("Tagged module disagrees with the prepared Maven identity.")
        name = pom.findtext("m:build/m:finalName", default="${project.artifactId}-${project.version}", namespaces=NS)
        name = name.replace("${project.artifactId}", module).replace("${project.version}", version)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name):
            raise signing.SigningError("Unsupported tagged artifact filename.")
        target = Path("target/checkout") / module / "target"
        if module == "scylla-cdc-replicator":
            assembly = next((p for p in pom.findall("m:build/m:plugins/m:plugin", NS)
                             if p.findtext("m:artifactId", namespaces=NS) == "maven-assembly-plugin"), None)
            if (assembly is None or assembly.findtext("m:configuration/m:outputDirectory", namespaces=NS) != "target/fat-jar/"
                    or assembly.findtext("m:configuration/m:descriptorRefs/m:descriptorRef", namespaces=NS) != "jar-with-dependencies"
                    or assembly.find("m:configuration/m:finalName", NS) is not None
                    or assembly.findtext("m:configuration/m:appendAssemblyId", default="true", namespaces=NS) != "true"):
                raise signing.SigningError("Unsupported tagged replicator assembly layout.")
            target /= "fat-jar"
            name += "-jar-with-dependencies"
        paths.append(target / (name + ".jar"))
    return paths


def api_json(*args):
    try:
        return json.loads(signing.run(["gh", "api", *args]))
    except (ValueError, signing.SigningError):
        raise signing.SigningError("GitHub release API failed; inspect remote state before recovery.") from None


def lookup(tag):
    query = ("query($owner:String!,$name:String!,$tag:String!){"
             "repository(owner:$owner,name:$name){release(tagName:$tag){id}}}")
    result = api_json("graphql", "-f", "query=" + query, "-F", "owner={owner}", "-F", "name={repo}", "-f", "tag=" + tag)
    data = result.get("data") if isinstance(result, dict) else None
    repository = data.get("repository") if isinstance(data, dict) else None
    if not isinstance(repository, dict) or "release" not in repository or result.get("errors"):
        raise signing.SigningError("Could not inspect GitHub release; no publication attempted.")
    if repository["release"] is not None:
        raise signing.SigningError("GitHub release or draft already exists; inspect and recover it manually.")


def verify_remote_tag(tag):
    expected = signing.run(["git", "rev-parse", "refs/tags/" + tag]).strip()
    remote = api_json("repos/{owner}/{repo}/git/ref/tags/" + tag)
    obj = remote.get("object") if isinstance(remote, dict) else None
    if not isinstance(obj, dict) or obj.get("type") != "tag" or obj.get("sha") != expected:
        raise signing.SigningError("Remote release tag differs from the prepared signed tag.")


def read_after_create(command, **kwargs):
    # Draft discovery can briefly lag creation. Retry only reads, never writes.
    for attempt in range(3):
        try:
            return signing.run(command, **kwargs)
        except signing.SigningError:
            if attempt == 2:
                raise
            time.sleep(1)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(65536), b""):
            digest.update(chunk)
    return digest.digest()


def release_state(tag, draft, release_id=None):
    state = json.loads(read_after_create(["gh", "release", "view", tag, "--json", "databaseId,tagName,isDraft"]))
    if (not isinstance(state, dict) or type(state.get("databaseId")) is not int
            or state["databaseId"] <= 0 or state.get("tagName") != tag or state.get("isDraft") is not draft
            or (release_id is not None and state["databaseId"] != release_id)):
        raise signing.SigningError("Unexpected GitHub release identity or draft state.")
    return state["databaseId"]


def verify_uploads(tag, release_id, files):
    pages = api_json("--paginate", "--slurp", f"repos/{{owner}}/{{repo}}/releases/{release_id}/assets?per_page=100")
    if not isinstance(pages, list) or not all(isinstance(page, list) for page in pages):
        raise signing.SigningError("Incomplete GitHub asset listing.")
    remote = [asset for page in pages for asset in page]
    expected = {path.name: path for path in files}
    if (len(expected) != 8 or len(remote) != len(expected)
            or any(not isinstance(asset, dict) for asset in remote)
            or {asset.get("name") for asset in remote} != set(expected)):
        raise signing.SigningError("GitHub draft must contain exactly all eight release assets.")
    for asset in remote:
        path = expected[asset["name"]]
        if asset.get("state") != "uploaded" or asset.get("size") != path.stat().st_size:
            raise signing.SigningError("GitHub asset upload is incomplete.")
    # Download the staged bytes: names and sizes alone do not detect tampering.
    with tempfile.TemporaryDirectory(prefix="release-download-") as temp:
        read_after_create(["gh", "release", "download", tag, "--clobber", "--dir", temp], timeout=1800)
        downloaded = Path(temp)
        if {path.name for path in downloaded.iterdir()} != set(expected):
            raise signing.SigningError("GitHub download is incomplete.")
        for name, local in expected.items():
            if sha256(local) != sha256(downloaded / name):
                raise signing.SigningError("Downloaded GitHub asset differs from its signed release file.")


def publish():
    tag, _ = identity()
    signing.verify_prepared()
    files = artifacts()
    if any(not Path(str(path) + ".asc").is_file() for path in files):
        raise signing.SigningError("Sign and verify all four GitHub binaries before publication.")
    signing.sign_files(files)
    files = [item for path in files for item in (path, Path(str(path) + ".asc"))]
    lookup(tag)
    verify_remote_tag(tag)
    notes = api_json("--method", "POST", "repos/{owner}/{repo}/releases/generate-notes", "-f", "tag_name=" + tag,
                     "-f", "configuration_file_path=.github/release.yml")
    if not isinstance(notes, dict) or not isinstance(notes.get("body"), str):
        raise signing.SigningError("Could not generate release notes.")
    with tempfile.TemporaryDirectory(prefix="release-notes-") as temp:
        body = Path(temp) / "notes.md"
        body.write_text(notes["body"])
        signing.run(["gh", "release", "create", tag, *map(str, files), "--verify-tag", "--draft",
                     "--title", tag, "--notes-file", str(body)], timeout=1800)
    release_id = release_state(tag, True)
    verify_uploads(tag, release_id, files)
    verify_remote_tag(tag)
    release_state(tag, True, release_id)
    signing.run(["gh", "release", "edit", tag, "--draft=false"], timeout=1800)
    release_state(tag, False, release_id)


def main():
    try:
        if sys.argv[1:] == ["identity"]:
            tag, version = identity()
            print(f"RELEASE_TAG={tag}\nRELEASE_VERSION={version}")
        elif sys.argv[1:] == ["sign"]:
            signing.sign_files(artifacts())
        elif sys.argv[1:] == ["publish"]:
            publish()
        else:
            raise signing.SigningError("Expected identity, sign, or publish.")
        return 0
    except signing.SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print("Release asset operation failed; values withheld.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
