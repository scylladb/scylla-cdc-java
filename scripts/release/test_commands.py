"""Exercise actual release recipes with a fake Maven executable and synthetic secrets."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest

import guard

ROOT = Path(__file__).resolve().parents[2]


def workflow_command(name):
    # Extract one named step from this repository's fixed workflow layout.
    workflow = (ROOT / ".github/workflows/release.yml").read_text()
    step = workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
    command = step.split("        run: ", 1)[1]
    return dedent(command[2:]) if command.startswith("|\n") else command.strip()


class ReleaseCommandTests(unittest.TestCase):
    def test_log_scanning_follows_scm_and_precedes_artifact_uploads(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(workflow.index("Upload signed recovery history"), workflow.index("Scan release logs"))
        self.assertLess(workflow.index("Scan release logs"), workflow.index("Upload sanitized stdout"))
        self.assertLess(workflow.index("Scan validation logs"), workflow.index("Upload sanitized validation stdout"))
        for name in ("Scan validation logs", "Scan release logs"):
            step = workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
            self.assertIn("if: always()", step)
            self.assertIn('python3 "$RELEASE_RUNNER" report', step)
        self.assertNotIn("release-logs-private", workflow)

    def test_release_accepts_only_master_dispatch(self):
        command = workflow_command("Check release branch")
        for ref in ("refs/heads/master", "refs/heads/feature", "refs/tags/master"):
            with self.subTest(ref=ref):
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", command],
                    env={**os.environ, "GITHUB_REF": ref},
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode == 0, ref == "refs/heads/master", result.stderr)


    def test_workflow_checks_trusted_target_before_credentials(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(workflow.index("Checkout validated target"), workflow.index("Check release configuration"))
        self.assertLess(workflow.index("Check release configuration"), workflow.index("RELEASE_GPG_PRIVATE_KEY:"))
        self.assertNotIn("gpg-private-key:", workflow)
        self.assertLess(workflow.index("Preserve release tooling"),
                        workflow.index("Checkout validated target"))
        self.assertIn("default: master", workflow)
        self.assertLess(workflow.index("Push changes to SCM"), workflow.index("Upload sanitized stdout"))
        self.assertEqual(workflow.count("if: always()"), 6)
        self.assertRegex(workflow, r"(?m)^    environment: release$")
        self.assertEqual(workflow_command("Prepare release").splitlines()[-1], 'python3 "$RELEASE_RUNNER" prepare --ci --defer-logs')
        self.assertIn("RELEASE_DRY_RUN: ${{ inputs.dry-run }}", workflow)
        self.assertEqual(workflow_command("Verify prepared release commits and tag with public key only"),
                         'python3 "$RELEASE_SIGNING_HELPER" verify-prepared')


    def test_workflow_keeps_scm_credentials_in_the_push_process_only(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertEqual(workflow.count("persist-credentials: false"), 2)
        push = workflow.split("      - name: Push changes to SCM\n", 1)[1].split(
            "\n      - name:", 1
        )[0]
        self.assertIn("GH_TOKEN: ${{ github.token }}", push)
        # Exercise the exact shell helper through Git's credential protocol.
        command = workflow_command("Push changes to SCM").replace(
            "git status && git log -3\n", ""
        ).replace("push origin --follow-tags -v HEAD:refs/heads/master", "credential fill")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.git(root, "init", "--quiet")
            for action in ("fill", "approve", "reject"):
                with self.subTest(action=action):
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c",
                         command.replace("credential fill", f"credential {action}")], cwd=root,
                        env={**os.environ, "GH_TOKEN": "synthetic-push-token"},
                        input=("protocol=https\nhost=github.com\n" +
                               ("" if action == "fill" else "username=fixture\npassword=fixture\n") + "\n"),
                        capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    if action == "fill":
                        self.assertIn("username=x-access-token", result.stdout)
                        self.assertIn("password=synthetic-push-token", result.stdout)
                    else:
                        self.assertEqual(result.stdout, "")
            config = (root / ".git/config").read_text()
            self.assertNotIn("credential", config)
            self.assertNotIn("synthetic-push-token", config)


    def test_failed_push_recovery_preserves_history_without_checkout_secrets(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        for name in ("Preserve signed history after failed SCM push", "Upload signed recovery history"):
            step = workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
            self.assertIn("if: ${{ failure() && steps.scm_push.outcome == 'failure' }}", step)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            tag = "scylla-cdc-1.0"
            self.git(source, "tag", "-a", tag, "-m", "Release", commits[1])
            tag_oid = self.git(source, "rev-parse", "refs/tags/" + tag)
            self.git(source, "config", "credential.fixture", "synthetic-checkout-secret")
            (source / "release.properties").write_text(
                f"scm.tag={tag}\nscm.password=synthetic-properties-secret\n")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c",
                 workflow_command("Preserve signed history after failed SCM push")],
                cwd=source, env={**os.environ, "RUNNER_TEMP": str(root),
                                 "RELEASE_BASE_COMMIT": commits[0]},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            recovery = root / "release-recovery"
            self.assertEqual({path.name for path in recovery.iterdir()},
                             {"history.bundle", "baseline", "head", "release-tag"})
            self.assertEqual((recovery / "baseline").read_text().strip(), commits[0])
            self.assertEqual((recovery / "head").read_text().strip(), commits[-1])
            self.assertEqual((recovery / "release-tag").read_text().strip(), tag)
            restored = root / "restored"
            self.git(root, "init", "--quiet", str(restored))
            self.git(restored, "fetch", str(recovery / "history.bundle"),
                     "HEAD:refs/heads/recovered", f"refs/tags/{tag}:refs/tags/{tag}")
            self.assertEqual(self.git(restored, "rev-parse", "recovered"), commits[-1])
            self.assertEqual(self.git(restored, "rev-parse", "refs/tags/" + tag), tag_oid)
            self.assertEqual(self.git(restored, "cat-file", "-t", tag_oid), "tag")
            self.assertNotIn("synthetic-checkout-secret", (restored / ".git/config").read_text())
            self.assertNotIn("release.properties", self.git(restored, "ls-tree", "-r", "recovered"))


    def test_validation_precedes_approval_on_a_separate_runner_without_release_secrets(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        validation, release = workflow.split("\n  release:\n", 1)
        release_header = release.split("    steps:", 1)[0]
        self.assertIn("\n  validate:\n", validation)
        self.assertIn("    runs-on: ubuntu-latest\n", validation)
        self.assertNotIn("environment:", validation)
        self.assertNotIn("secrets.", validation)
        self.assertIn("      contents: read\n", validation)
        self.assertNotIn("contents: write", validation)
        self.assertIn("    needs: validate\n", release_header)
        self.assertIn("    runs-on: ubuntu-latest\n", release_header)
        self.assertIn("    environment: release\n", release_header)
        self.assertNotRegex(release_header, r"(?m)^    if:")
        self.assertIn("RELEASE_TARGET_COMMIT: ${{ needs.validate.outputs.target-sha }}", release)
        self.assertNotIn("cache: maven", release)
        self.assertEqual(release.count("RELEASE_SKIP_TESTS: 'true'"), 2)
        self.assertIn("python3 -m unittest discover", validation)
        self.assertNotIn("ci-release", validation)
        self.assertIn("ref: ${{ github.sha }}", release)
        self.assertIn("git fetch --no-tags origin \"$RELEASE_TARGET_COMMIT\"", release)


    def test_validation_builds_unsigned_release_artifacts_without_deployment(self):
        command = workflow_command("Validate target without release credentials")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            maven = root / "mvn"
            maven.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "assert not any(os.environ.get(key) for key in "
                "('RELEASE_GPG_PRIVATE_KEY', 'MAVEN_GPG_PASSPHRASE', "
                "'SONATYPE_TOKEN_USERNAME', 'SONATYPE_TOKEN_PASSWORD'))\n"
                "Path(os.environ['TRACE']).write_text(json.dumps("
                "{'args': sys.argv[1:], 'opts': os.environ.get('MAVEN_OPTS', '')}))\n"
            )
            maven.chmod(0o700)
            env = {key: value for key, value in os.environ.items()
                   if not key.startswith("RELEASE_") and key not in guard.ENV_OPTIONS
                   and key not in ("MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_USERNAME",
                                   "SONATYPE_TOKEN_PASSWORD")}
            env.update(PATH=str(root) + os.pathsep + os.environ["PATH"],
                       RELEASE_RUNNER=str(ROOT / "scripts/release/release.py"),
                       RELEASE_LOG_DIR=str(root / "logs"), TRACE=str(root / "trace"))
            for skip in ("false", "true"):
                with self.subTest(skip=skip):
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c", command], cwd=ROOT,
                        env={**env, "RELEASE_SKIP_TESTS": skip},
                        capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    call = json.loads((root / "trace").read_text())
                    self.assertEqual(call["args"], ["-B", "--no-transfer-progress", "-Prelease",
                                                    "-Dgpg.skip=true", "verify"])
                    self.assertEqual("-DskipTests=true" in call["opts"], skip == "true")


    def target_fixture(self, root):
        source = root / "source"
        source.mkdir()
        self.git(source, "init", "--quiet", "--initial-branch=master")
        commits = []
        for version in ("initial", "release", "development"):
            (source / "version").write_text(version)
            self.git(source, "add", "version")
            self.git(source, "commit", "--quiet", "-m", version)
            commits.append(self.git(source, "rev-parse", "HEAD"))
            if version == "release":
                self.git(source, "tag", "v1.0")
        return source, commits


    def git(self, directory, *args):
        return subprocess.check_output(
            ["git", "-c", "commit.gpgsign=false", "-c", "tag.gpgSign=false",
             "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
             "-C", str(directory), *args], text=True, stderr=subprocess.PIPE,
        ).strip()


    def checkout_validated_target(self, root, source, expected, tag="master"):
        resolve = workflow_command("Record validated commit")
        checkout = workflow_command("Checkout validated target")
        validation = root / "validation"
        self.git(root, "clone", "--quiet", str(source), str(validation))
        output = root / "output"
        result = subprocess.run(
            ["bash", "-eo", "pipefail", "-c", resolve], cwd=validation,
            env={**os.environ, "GITHUB_OUTPUT": str(output), "RELEASE_TARGET_TAG": tag},
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_text(), f"sha={expected}\n")
        publisher = root / "publisher"
        self.git(root, "clone", "--quiet", "--no-tags", str(source), str(publisher))
        return checkout, publisher


    def test_publishing_main_uses_validated_commit_and_rejects_an_advanced_branch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            checkout, publisher = self.checkout_validated_target(root, source, commits[-1])
            env = {**os.environ, "GITHUB_REF": "refs/heads/master",
                   "RELEASE_TARGET_COMMIT": commits[-1], "RELEASE_TARGET_TAG": "master"}
            result = subprocess.run(["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), commits[-1])
            self.assertEqual(self.git(publisher, "branch", "--show-current"), "master")
            for ref in ("refs/heads/feature", "refs/tags/master"):
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                    env={**env, "GITHUB_REF": ref}, capture_output=True, text=True, timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Releases must be dispatched from master", result.stdout)
            self.git(source, "commit", "--quiet", "--allow-empty", "-m", "master advanced")
            result = subprocess.run(["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("master advanced after validation", result.stdout)


    def test_publication_rechecks_master_after_local_release_preparation(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(workflow.index("Recheck for open release blockers"),
                        workflow.index("Recheck validated master before publication"))
        self.assertEqual(
            workflow.split("      - name: Recheck validated master before publication\n", 1)[1]
            .split("\n      - name:", 1)[1].splitlines()[0], " Perform release",
        )
        recheck = workflow_command("Recheck validated master before publication")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            env = dict(os.environ)
            source, commits = self.target_fixture(root)
            checkout, publisher = self.checkout_validated_target(root, source, commits[-1])
            env.update(GITHUB_REF="refs/heads/master", RELEASE_TARGET_COMMIT=commits[-1],
                       RELEASE_TARGET_TAG="master")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.git(publisher, "commit", "--quiet", "--allow-empty", "-m", "prepared release")
            prepared = self.git(publisher, "rev-parse", "HEAD")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", recheck], cwd=publisher,
                env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), prepared)
            make = root / "python3"
            make.write_text('#!/bin/sh\ntouch "$PERFORM_TRACE"\n')
            make.chmod(0o755)
            env["PATH"] = str(root) + os.pathsep + os.environ["PATH"]
            env.update(PERFORM_TRACE=str(root / "performed"), RELEASE_DRY_RUN="false")
            self.git(source, "commit", "--quiet", "--allow-empty", "-m", "master advanced")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c",
                 recheck + "\n" + workflow_command("Perform release")], cwd=publisher,
                env=env, capture_output=True, text=True, timeout=10,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("master advanced after validation", result.stdout)
            self.assertFalse((root / "performed").exists())
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), prepared)


    def test_publishing_historical_target_does_not_resolve_a_moved_tag_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            checkout, publisher = self.checkout_validated_target(root, source, commits[0], tag="v1.0")
            self.git(source, "tag", "--force", "v1.0", commits[-1])
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                env={**os.environ, "GITHUB_REF": "refs/heads/master", "RELEASE_TARGET_TAG": "v1.0",
                     "RELEASE_TARGET_COMMIT": commits[0]},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(publisher, "rev-parse", "HEAD"), commits[0])
            self.assertEqual(self.git(publisher, "tag", "--list"), "")
            self.assertEqual((publisher / "version").read_text(), "initial")
            remote_refs = self.git(source, "show-ref")
            self.git(publisher, "commit", "--quiet", "--allow-empty", "-m", "historical preparation")
            self.git(publisher, "tag", "v1.0")
            self.assertEqual(self.git(source, "show-ref"), remote_refs)
            workflow = (ROOT / ".github/workflows/release.yml").read_text()
            push = workflow.split("      - name: Push changes to SCM\n", 1)[1].split(
                "\n      - name:", 1
            )[0]
            self.assertIn("if: ${{ inputs.dry-run == false && inputs.target-tag == 'master' }}", push)
            self.assertIn("if: inputs.target-tag == 'master'", workflow)


    def test_publishing_main_pushes_from_a_sha_checkout_without_an_upstream(self):
        push = workflow_command("Push changes to SCM")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, commits = self.target_fixture(root)
            remote = root / "remote.git"
            self.git(root, "clone", "--quiet", "--bare", str(source), str(remote))
            checkout, publisher = self.checkout_validated_target(root, source, commits[-1])
            self.git(publisher, "remote", "set-url", "origin", str(remote))
            # A full-SHA checkout starts detached and creates no tracking branch.
            self.git(publisher, "checkout", "--detach")
            self.git(publisher, "branch", "-D", "master")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", checkout], cwd=publisher,
                env={**os.environ, "GITHUB_REF": "refs/heads/master",
                     "RELEASE_TARGET_COMMIT": commits[-1], "RELEASE_TARGET_TAG": "master"},
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.git(publisher, "commit", "--quiet", "--allow-empty", "-m", "prepared release")
            self.git(publisher, "tag", "-a", "release-fixture", "-m", "prepared release")
            prepared = self.git(publisher, "rev-parse", "HEAD")
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", push], cwd=publisher,
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.git(remote, "rev-parse", "refs/heads/master"), prepared)
            self.assertEqual(self.git(remote, "rev-parse", "refs/tags/release-fixture^{}"), prepared)



if __name__ == "__main__":
    unittest.main()
