"""Exercise actual release recipes with a fake Maven executable and synthetic secrets."""

import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
from textwrap import dedent
import unittest
from unittest.mock import patch

import guard

ROOT = Path(__file__).resolve().parents[2]


def workflow_command(name):
    # Extract one named step from this repository's fixed workflow layout.
    workflow = (ROOT / ".github/workflows/release.yml").read_text()
    step = workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - name:", 1)[0]
    command = step.split("        run: ", 1)[1]
    return dedent(command[2:]) if command.startswith("|\n") else command.strip()


class ReleaseCommandTests(unittest.TestCase):
    def fake_environment(self, directory):
        root = Path(directory)
        binary = root / "mvn"
        binary.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "with open(os.environ['FAKE_MAVEN_TRACE'], 'a') as trace:\n"
            " trace.write(json.dumps({'args': sys.argv[1:], "
            "'passphrase': os.environ['MAVEN_GPG_PASSPHRASE'], "
            "'opts': os.environ.get('MAVEN_OPTS', ''), "
            "'maven_args': os.environ.get('MAVEN_ARGS', ''), 'cwd': os.getcwd(), "
            "'baseline': os.environ.get('RELEASE_BASE_COMMIT', '')}) + '\\n')\n"
            "print('stdout: ' + os.environ['MAVEN_GPG_PASSPHRASE'])\n"
            "print('stderr: ' + os.environ['MAVEN_GPG_PASSPHRASE'], file=sys.stderr)\n"
            "raise SystemExit(int(os.environ.get('FAKE_MAVEN_STATUS', '0')))\n"
        )
        binary.chmod(0o755)
        gh = root / "gh"
        gh.write_text('#!/bin/sh\nprintf "%s\\n" scylladb-publisher\n')
        gh.chmod(0o755)
        helper = root / "signing.py"
        helper.write_text(
            "import os, pathlib, subprocess, sys\n"
            "if 'FAKE_PUBLISHER_CERT' in os.environ:\n"
            " assert pathlib.Path(__file__).with_name('publisher.asc').read_text() "
            "== os.environ['FAKE_PUBLISHER_CERT']\n"
            "if 'FAKE_RELEASE_BASE' in os.environ:\n"
            " assert os.environ.get('RELEASE_BASE_COMMIT') == os.environ['FAKE_RELEASE_BASE']\n"
            "if sys.argv[1] == 'exec':\n"
            " raise SystemExit(subprocess.run(sys.argv[2:]).returncode)\n"
        )
        env = {key: value for key, value in os.environ.items() if key not in guard.ENV_OPTIONS}
        env.update(
            PATH=str(root) + os.pathsep + env.get("PATH", ""),
            PYTHONDONTWRITEBYTECODE="1",
            MAVEN_GPG_PASSPHRASE="synthetic-passphrase",
            SONATYPE_TOKEN_USERNAME="synthetic-user",
            SONATYPE_TOKEN_PASSWORD="synthetic-password",
            RELEASE_LOG_DIR=str(root / "logs"),
            FAKE_MAVEN_TRACE=str(root / "trace.jsonl"),
            RELEASE_SKIP_TESTS="false",
            RELEASE_VERSION="",
            IS_CICD="true",
            GITHUB_ACTIONS="true",
            GITHUB_ACTOR="release-operator",
            GITHUB_TRIGGERING_ACTOR="rerun-operator",
            RELEASE_SIGNING_HELPER=str(helper),
            RELEASE_GUARD=str(ROOT / "scripts/release/guard.py"),
            RELEASE_MAKEFILE=str(ROOT / "Makefile"),
        )
        return root, env

    def run_make(self, target, env, *options):
        return subprocess.run(
            ["make", "--no-print-directory", *options, target], cwd=ROOT, env=env,
            capture_output=True, text=True, timeout=10,
        )

    def test_make_previews_never_execute_release_tools(self):
        targets = (
            "release-prepare", "release", "release-dry-run",
            "ci-release-prepare", "ci-release", "ci-release-dry-run",
            "_release-prepare", "_release", "_release-dry-run",
            "_ci-release-prepare", "_ci-release", "_ci-release-dry-run",
        )
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["PREVIEW_TRACE"] = str(root / "preview-trace")
            # Python is the entry point for both guard and signing helpers.
            for tool in ("gh", "python3", "mvn"):
                executable = root / tool
                executable.write_text(
                    '#!/bin/sh\nprintf "%s\\n" "$0" >> "$PREVIEW_TRACE"\nexit 93\n'
                )
                executable.chmod(0o755)
            for option in ("-n", "--just-print", "--dry-run"):
                for target in targets:
                    with self.subTest(option=option, target=target):
                        result = self.run_make(target, env, option)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertFalse((root / "preview-trace").exists(), result.stdout)
                        self.assertFalse((root / "trace.jsonl").exists())
                        self.assertIn("release:prepare" if target.endswith("prepare")
                                      else "release:perform", result.stdout)

    def test_all_release_phases_capture_both_streams_and_keep_credentials_in_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            for target in ("release-prepare", "release", "release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertIn("release:prepare", calls[0]["args"])
            self.assertIn("release:perform", calls[1]["args"])
            self.assertIn("release:perform", calls[2]["args"])
            self.assertIn("-Drelease.autopublish=true", calls[1]["args"])
            self.assertIn("-Drelease.autopublish=false", calls[2]["args"])
            for call in calls:
                self.assertEqual(call["passphrase"], env["MAVEN_GPG_PASSPHRASE"])
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], " ".join(call["args"]))
                self.assertNotIn("-DskipTests=true", call["opts"])
            self.assertEqual((root / "logs/stdout.log").read_text(), "stdout: [REDACTED]\n" * 3)
            self.assertEqual((root / "logs/stderr.log").read_text(), "stderr: [REDACTED]\n" * 3)

    def test_internal_local_targets_accept_authenticated_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            for target in ("_release-prepare", "_release", "_release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)

    def test_explicit_ci_targets_share_release_phases_without_publisher_gh_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            (root / "gh").write_text('#!/bin/sh\nexit 99\n')
            for target in ("ci-release-prepare", "ci-release", "ci-release-dry-run",
                           "_ci-release-prepare", "_ci-release", "_ci-release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 6)
            for index, call in enumerate(calls):
                self.assertIn("release:prepare" if index % 3 == 0 else "release:perform", call["args"])
                self.assertEqual("-Drelease.autopublish=true" in call["args"], index % 3 == 1)
                self.assertEqual("-Drelease.autopublish=false" in call["args"], index % 3 == 2)

    def test_perform_phases_override_conflicting_inherited_publication_settings(self):
        for target, publish in (
            ("release-dry-run", "false"), ("ci-release-dry-run", "false"),
            ("_release-dry-run", "false"), ("_ci-release-dry-run", "false"),
            ("release", "true"), ("ci-release", "true"),
            ("_release", "true"), ("_ci-release", "true"),
            ("workflow-dry-run", "false"), ("workflow-release", "true"),
        ):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root, env = self.fake_environment(directory)
                inherited = "true" if publish == "false" else "false"
                env["MAVEN_ARGS"] = f"-Drelease.autopublish={inherited}"
                env["MAVEN_OPTS"] = f"-Xmx256m -Drelease.autopublish={inherited}"
                if target.startswith("workflow-"):
                    result = subprocess.run(
                        ["bash", "-eo", "pipefail", "-c", workflow_command("Perform release")],
                        cwd=ROOT, env={**env, "RELEASE_DRY_RUN": inherited},
                        capture_output=True, text=True, timeout=10,
                    )
                else:
                    result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                call = json.loads((root / "trace.jsonl").read_text())
                self.assertEqual(
                    [arg for arg in call["args"] if arg.startswith("-Drelease.autopublish=")],
                    [f"-Drelease.autopublish={publish}"],
                )
                self.assertEqual(call["maven_args"], env["MAVEN_ARGS"])
                self.assertEqual(call["opts"], env["MAVEN_OPTS"])

    def test_public_key_selection_preserves_literal_jvm_options_for_every_release_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            key_option = "-Dgpg.keyname=" + "A" * 40 + "!"
            marker = root / "must-not-exist"
            env["MAVEN_OPTS"] = (
                "-Xmx512m -Dhome=$HOME -Dbraced=${HOME} "
                f"'-Dmake=$(shell touch {marker})' " + key_option
            )
            for target in ("release-prepare", "release", "release-dry-run"):
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            for call in calls:
                self.assertEqual(call["opts"], env["MAVEN_OPTS"])
            self.assertFalse(marker.exists())

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

    def test_workflow_uses_preserved_recipes_in_target_checkout(self):
        perform = workflow_command("Perform release")
        commands = [workflow_command("Prepare release"),
                    workflow_command("Verify prepared release commits and tag with public key only"),
                    'export RELEASE_DRY_RUN=false\n' + perform,
                    'export RELEASE_DRY_RUN=true\n' + perform]
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            target = root / "target-checkout"
            target.mkdir()
            shutil.copy2(ROOT / "pom.xml", target / "pom.xml")
            original_pom = (target / "pom.xml").read_text()
            marker = target / "unsafe-target-executed"
            # Any read of target Makefiles, including parse-time shell execution,
            # would create the marker and expose the passphrase before capture.
            unsafe = (f"$(shell touch {marker})\nSHELL := bash\n.SHELLFLAGS := -x -c\n"
                      "override RELEASE_GUARD := unsafe.py\n"
                      "release-prepare release release-dry-run:\n"
                      "\techo $${MAVEN_GPG_PASSPHRASE}\n")
            (target / "GNUmakefile").write_text(unsafe)
            (target / "Makefile").write_text(
                'release-prepare release release-dry-run:\n\ttouch "' + str(marker) + '"\n')
            (target / "scripts/release").mkdir(parents=True)
            (target / "scripts/release/signing.py").write_text(
                f"from pathlib import Path\nPath({str(marker)!r}).touch()\nraise SystemExit(1)\n")
            (target / "scripts/release/publisher.asc").write_text("unsafe-target-certificate\n")
            (target / "scripts/release/guard.py").write_text(
                f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
            subprocess.run(["git", "init", "--quiet", str(target)], check=True, capture_output=True)
            subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                            "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", "Fixture"],
                           cwd=target, check=True, capture_output=True)
            baseline = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=target, text=True).strip()
            preserved = root / "release-tooling"
            fake_signing = env["RELEASE_SIGNING_HELPER"]
            env.update(RUNNER_TEMP=str(root), GITHUB_ENV=str(root / "github-env"),
                       FAKE_RELEASE_BASE=baseline)
            for name in ("Preserve validation tooling", "Preserve release tooling"):
                (root / "github-env").write_text("")
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", workflow_command(name)], cwd=ROOT,
                    env=env, capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                for filename in ("guard.py", "signing.py", "publisher.asc"):
                    self.assertEqual((preserved / filename).read_bytes(),
                                     (ROOT / "scripts/release" / filename).read_bytes())
                self.assertEqual((preserved / "Makefile").read_bytes(),
                                 (ROOT / "Makefile").read_bytes())
            for assignment in (root / "github-env").read_text().splitlines():
                name, value = assignment.split("=", 1)
                env[name] = value
            (root / "github-env").write_text("")
            shutil.copy2(fake_signing, preserved / "signing.py")
            env["FAKE_PUBLISHER_CERT"] = (preserved / "publisher.asc").read_text()
            # Execute the precredential step with the selected source's POM.
            check_env = {key: value for key, value in env.items()
                         if key not in ("MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_USERNAME",
                                        "SONATYPE_TOKEN_PASSWORD", "RELEASE_GPG_PRIVATE_KEY")}
            check = workflow_command("Check release credential safety before importing credentials")
            for unsafe in (False, True):
                (target / "pom.xml").write_text(original_pom.replace(
                    "<gpg.bestPractices>true", "<gpg.bestPractices>false"
                ) if unsafe else original_pom)
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", check], cwd=target, env=check_env,
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode == 0, not unsafe, result.stderr)
                self.assertFalse(marker.exists())
            (target / "pom.xml").write_text(original_pom)
            for command in commands:
                result = subprocess.run(
                    ["bash", "-eo", "pipefail", "-c", command], cwd=target, env=env,
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)
                self.assertFalse(marker.exists())
                # Actions makes public GITHUB_ENV values available to later steps.
                public_state = (root / "github-env").read_text()
                self.assertEqual(public_state, f"RELEASE_BASE_COMMIT={baseline}\n")
                env["RELEASE_BASE_COMMIT"] = public_state.strip().split("=", 1)[1]
            calls = [json.loads(line) for line in (root / "trace.jsonl").read_text().splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertTrue(all(call["cwd"] == str(target) for call in calls))
            self.assertTrue(all(call["baseline"] == baseline for call in calls))
            self.assertEqual((target / "pom.xml").read_text(), original_pom)
            for name in ("stdout", "stderr"):
                output = (Path(env["RELEASE_LOG_DIR"]) / f"{name}.log").read_text()
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], output)
                self.assertEqual(output.count("[REDACTED]"), 3)

    def test_passphrase_quotes_and_shell_syntax_never_become_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            marker = root / "must-not-exist"
            env["MAVEN_GPG_PASSPHRASE"] = f'synthetic "quoted" $(touch {marker}) secret'
            result = self.run_make("release-prepare", env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(marker.exists())
            call = json.loads((root / "trace.jsonl").read_text())
            self.assertEqual(call["passphrase"], env["MAVEN_GPG_PASSPHRASE"])
            self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], result.stdout + result.stderr)

    def test_requested_skip_flags_apply_to_prepare_perform_and_dry_run(self):
        for target in ("release-prepare", "release", "release-dry-run"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root, env = self.fake_environment(directory)
                env["RELEASE_SKIP_TESTS"] = "true"
                result = self.run_make(target, env)
                self.assertEqual(result.returncode, 0, result.stderr)
                call = json.loads((root / "trace.jsonl").read_text())
                self.assertIn("-DskipTests=true", call["opts"])
                self.assertIn("-DskipITs=true", call["opts"])

    def test_version_override_remains_one_argument(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["RELEASE_VERSION"] = "2.0.8"
            result = self.run_make("release-prepare", env)
            self.assertEqual(result.returncode, 0, result.stderr)
            call = json.loads((root / "trace.jsonl").read_text())
            self.assertIn("-DreleaseVersion=2.0.8", call["args"])

    def test_maven_failure_keeps_sanitized_artifacts_and_fails_make(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["FAKE_MAVEN_STATUS"] = "17"
            result = self.run_make("release", env)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Error 17", result.stderr)
            for name in ("stdout", "stderr"):
                output = (root / f"logs/{name}.log").read_text()
                self.assertIn("[REDACTED]", output)
                self.assertNotIn(env["MAVEN_GPG_PASSPHRASE"], output)

    def test_debug_environment_is_blocked_before_fake_maven_starts(self):
        with tempfile.TemporaryDirectory() as directory:
            root, env = self.fake_environment(directory)
            env["MAVEN_ARGS"] = "-B -X"
            result = self.run_make("release", env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / "trace.jsonl").exists())

    def test_unsafe_environment_is_rejected_during_precredential_check(self):
        with patch.dict(os.environ, {"MAVEN_ARGS": "-B -X"}, clear=True), patch.object(
            sys, "argv", ["guard.py", "check"]
        ), patch.object(guard, "check_policy") as policy:
            self.assertEqual(guard.main(), 2)
            policy.assert_not_called()

    def test_json_escaped_credentials_are_removed_before_logs(self):
        secret = 'synthetic "quote" \\ slash\nline é'
        forms = guard.secret_forms({"MAVEN_GPG_PASSPHRASE": secret})
        for ascii_only in (True, False):
            serialized = json.dumps(secret, ensure_ascii=ascii_only)
            with self.subTest(ascii_only=ascii_only):
                cleaned = guard.redact(serialized.encode(), forms)
                self.assertEqual(cleaned, b'"[REDACTED]"')

    def test_workflow_checks_trusted_target_before_credentials(self):
        workflow = (ROOT / ".github/workflows/release.yml").read_text()
        self.assertLess(workflow.index("Checkout validated target"), workflow.index("Check release credential safety"))
        self.assertLess(workflow.index("Check release credential safety"), workflow.index("RELEASE_GPG_PRIVATE_KEY:"))
        self.assertNotIn("gpg-private-key:", workflow)
        self.assertLess(workflow.index("Preserve release tooling"),
                        workflow.index("Checkout validated target"))
        self.assertIn("default: master", workflow)
        self.assertLess(workflow.index("Push changes to SCM"), workflow.index("Upload sanitized stdout"))
        self.assertEqual(workflow.count("if: always()"), 4)
        self.assertRegex(workflow, r"(?m)^    environment: release$")
        self.assertEqual(workflow_command("Prepare release").splitlines()[-1], 'make -f "$RELEASE_MAKEFILE" ci-release-prepare')
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
                       RELEASE_GUARD=str(ROOT / "scripts/release/guard.py"),
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
            root, env = self.fake_environment(directory)
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
            make = root / "make"
            make.write_text('#!/bin/sh\ntouch "$PERFORM_TRACE"\n')
            make.chmod(0o755)
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
