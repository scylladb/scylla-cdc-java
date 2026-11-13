"""Run the release entrypoint against a fake foreground Maven process."""

from contextlib import contextmanager, redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import guard
import logs
import release
import signing

ROOT = Path(__file__).resolve().parents[2]


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory()
        self.addCleanup(fixture.cleanup)
        self.root = Path(fixture.name)
        self.directory = self.root / "logs"
        binary = self.root / "mvn"
        binary.write_text(f"#!{sys.executable}\n" +
                          "import json, os, sys\nfrom pathlib import Path\n"
                          "Path(os.environ['TRACE']).write_text(json.dumps({'args':sys.argv[1:], 'env':dict(os.environ)}))\n"
                          "print('ordinary maven output')\n"
                          "print(os.environ.get('MAVEN_GPG_PASSPHRASE','no credentials'), file=sys.stderr)\n"
                          "raise SystemExit(int(os.environ.get('MAVEN_STATUS', '0')))\n")
        binary.chmod(0o700)
        env = {name: value for name, value in os.environ.items()
               if not name.startswith(("RELEASE_", "SONATYPE_"))
               and name not in (*guard.ENV_OPTIONS, "MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE")}
        env.update(PATH=str(self.root) + os.pathsep + env["PATH"], TRACE=str(self.root / "trace"),
                   RELEASE_LOG_DIR=str(self.directory), GITHUB_ACTIONS="true")
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, env, clear=True).start()
        self.verify = patch.object(signing, "verify_prepared").start()
        self.sessions = []

        @contextmanager
        def session(env):
            self.sessions.append(dict(env))
            yield {**env, "MAVEN_GPG_PASSPHRASE": "disposable-passphrase"}

        patch.object(signing, "signing_session", session).start()

    def invoke(self, phase, *args):
        with patch.object(sys, "argv", ["release.py", phase, "--ci", *args]), redirect_stdout(StringIO()):
            return release.main()

    def trace(self):
        return json.loads((self.root / "trace").read_text())

    def test_validation_removes_credentials_and_inherited_options(self):
        os.environ.update({name: "-Dunsafe=sensitive-value" for name in guard.ENV_OPTIONS})
        os.environ.update(RELEASE_GPG_PRIVATE_KEY="private", MAVEN_GPG_PASSPHRASE="password",
                          SONATYPE_TOKEN_PASSWORD="token", GH_TOKEN="push-token", RELEASE_SKIP_TESTS="true")
        self.assertEqual(self.invoke("validate"), 0)
        trace = self.trace()
        self.assertEqual(trace["args"], ["-B", "--no-transfer-progress", "-Prelease", "-Dgpg.skip=true", "verify"])
        self.assertEqual(trace["env"]["MAVEN_OPTS"], "-DskipTests=true -DskipITs=true")
        self.assertEqual(trace["env"]["MAVEN_SKIP_RC"], "true")
        for name in ("GH_TOKEN", "MAVEN_ARGS", "JAVA_TOOL_OPTIONS", "RELEASE_GPG_PRIVATE_KEY",
                     "MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_PASSWORD"):
            self.assertNotIn(name, trace["env"])
        self.assertFalse(self.sessions)
        self.verify.assert_not_called()

    def test_prepare_verifies_before_session_closes_and_defers_log_scan(self):
        os.environ["RELEASE_VERSION"] = "1.2.3"
        os.environ["SONATYPE_TOKEN_PASSWORD"] = "central-token"
        self.assertEqual(self.invoke("prepare", "--defer-logs"), 0)
        self.assertIn("-DreleaseVersion=1.2.3", self.trace()["args"])
        self.assertIn("-DpushChanges=false", self.trace()["args"])
        self.assertNotIn("SONATYPE_TOKEN_PASSWORD", self.sessions[0])
        self.verify.assert_called_once()
        self.assertFalse(self.directory.exists())
        self.assertTrue(logs.raw_directory(self.directory).exists())

    def test_perform_verifies_history_before_requesting_private_credentials(self):
        self.verify.side_effect = signing.SigningError("unsigned history")
        self.assertEqual(self.invoke("perform"), 2)
        self.assertFalse(self.sessions)
        self.assertFalse((self.root / "trace").exists())

    def test_dry_run_cannot_automatically_publish(self):
        self.assertEqual(self.invoke("dry-run"), 0)
        self.assertIn("-Drelease.autopublish=false", self.trace()["args"])
        self.verify.assert_called_once_with()

    def test_failed_maven_status_survives_reporting_failure(self):
        os.environ["MAVEN_STATUS"] = "17"
        with patch.object(logs, "report", side_effect=OSError("unsafe-error")):
            self.assertEqual(self.invoke("prepare"), 17)
        self.verify.assert_not_called()
        self.assertFalse(logs.raw_directory(self.directory).exists())

    def test_success_survives_reporting_failure(self):
        with patch.object(logs, "report", side_effect=OSError("unsafe-error")):
            self.assertEqual(self.invoke("perform"), 0)

    def test_broken_console_does_not_replace_maven_status(self):
        with patch("builtins.print", side_effect=BrokenPipeError), patch.object(os, "dup2"):
            self.assertEqual(self.invoke("perform"), 0)

    def test_completion_marker_failure_preserves_maven_result_and_withholds_logs(self):
        for status in (0, 17):
            os.environ["MAVEN_STATUS"] = str(status)
            with self.subTest(status=status), patch.object(Path, "touch", side_effect=OSError("disk full")):
                self.assertEqual(self.invoke("prepare"), status)
            self.assertIn("withheld", (self.directory / "stdout.log").read_text())
            self.assertFalse(logs.raw_directory(self.directory).exists())

    def test_bad_version_is_rejected_before_signing_or_maven(self):
        os.environ["RELEASE_VERSION"] = "1.2.3 -Drelease.autopublish=true"
        self.assertEqual(self.invoke("prepare"), 2)
        self.assertFalse(self.sessions)
        self.assertFalse((self.root / "trace").exists())

    def test_make_preview_runs_no_release_programs(self):
        sentinel = self.root / "sentinel.py"
        sentinel.write_text("raise SystemExit('must not execute')\n")
        for target in ("release-prepare", "release", "release-dry-run", "ci-release",
                       "_release-prepare", "_ci-release-dry-run"):
            result = subprocess.run(["make", "-n", target, "RELEASE_RUNNER=" + str(sentinel)],
                                    cwd=ROOT, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertNotIn("must not execute", result.stdout + result.stderr)

    def test_custom_option_files_are_rejected_without_interpreting_them(self):
        config = self.root / ".mvn"
        config.mkdir()
        for name in ("maven.config", "jvm.config"):
            path = config / name
            path.write_text("-Dperfectly.valid=but-unsupported\n")
            with self.assertRaises(guard.Unsafe):
                guard.check_config_files(self.root)
            path.unlink()

    def test_maven_ancestor_option_files_cannot_bypass_preflight(self):
        config = self.root / ".mvn"
        config.mkdir()
        (config / "jvm.config").write_text("-Dunsafe=value\n")
        source = self.root / "source"
        source.mkdir()
        with self.assertRaises(guard.Unsafe):
            guard.check_config_files(source)
        # An empty nearer .mvn stops the launcher's ancestor search.
        (source / ".mvn").mkdir()
        guard.check_config_files(source)

    def test_logs_inside_checkout_are_rejected_before_maven_clean(self):
        with self.assertRaisesRegex(guard.Unsafe, "outside the source checkout"):
            release.run_phase("prepare", ROOT / "target/release-logs")
        self.assertFalse(self.sessions)
        self.assertFalse((self.root / "trace").exists())


if __name__ == "__main__":
    unittest.main()
