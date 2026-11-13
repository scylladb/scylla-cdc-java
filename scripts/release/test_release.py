"""Exercise the shared Make release recipes with foreground fake Maven commands."""

from contextlib import redirect_stdout
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
        helper = self.root / "signing.py"
        helper.write_text(
            "from contextlib import contextmanager\nimport os, sys\nfrom pathlib import Path\n"
            f"sys.path.insert(0, {str(ROOT / 'scripts/release')!r})\nimport signing\n"
            "@contextmanager\ndef session(env):\n"
            " Path(os.environ['SESSION_TRACE']).touch()\n"
            " yield {**env, 'MAVEN_GPG_PASSPHRASE':'disposable-passphrase'}\n"
            "def verify(*args):\n"
            " if os.environ.get('FAIL_VERIFY'): raise signing.SigningError('unsigned history')\n"
            " with open(os.environ['VERIFY_TRACE'],'a') as trace: trace.write('verified\\n')\n"
            "signing.signing_session=session\nsigning.verify_prepared=verify\n"
            "raise SystemExit(signing.main())\n")
        env = {name: value for name, value in os.environ.items()
               if not name.startswith(("RELEASE_", "SONATYPE_"))
               and name not in (*guard.ENV_OPTIONS, "MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE")}
        env.update(PATH=str(self.root) + os.pathsep + env["PATH"], TRACE=str(self.root / "trace"),
                   RELEASE_LOG_DIR=str(self.directory), GITHUB_ACTIONS="true",
                   RELEASE_GUARD=str(ROOT / "scripts/release/guard.py"),
                   RELEASE_SIGNING_HELPER=str(helper),
                   RELEASE_LOG_HELPER=str(ROOT / "scripts/release/logs.py"),
                   SESSION_TRACE=str(self.root / "session"), VERIFY_TRACE=str(self.root / "verified"))
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, env, clear=True).start()

    def invoke(self, target, *args):
        return subprocess.run(["make", "--no-print-directory", "-f", str(ROOT / "Makefile"), target, *args],
                              cwd=ROOT, capture_output=True, text=True, timeout=10)

    def trace(self):
        return json.loads((self.root / "trace").read_text())

    def test_validation_removes_credentials_and_inherited_options(self):
        os.environ.update({name: "-Dunsafe=sensitive-value" for name in guard.ENV_OPTIONS})
        os.environ.update(RELEASE_GPG_PRIVATE_KEY="private", MAVEN_GPG_PASSPHRASE="password",
                          SONATYPE_TOKEN_PASSWORD="token", GH_TOKEN="push-token", RELEASE_SKIP_TESTS="true")
        result = self.invoke("release-validate")
        self.assertEqual(result.returncode, 0, result.stderr)
        trace = self.trace()
        self.assertEqual(trace["args"], ["-B", "--no-transfer-progress", "-Prelease", "-Dgpg.skip=true", "verify"])
        self.assertEqual(trace["env"]["MAVEN_OPTS"], "-DskipTests=true -DskipITs=true")
        self.assertEqual(trace["env"]["MAVEN_SKIP_RC"], "true")
        for name in ("GH_TOKEN", "MAVEN_ARGS", "JAVA_TOOL_OPTIONS", "RELEASE_GPG_PRIVATE_KEY",
                     "MAVEN_GPG_PASSPHRASE", "SONATYPE_TOKEN_PASSWORD"):
            self.assertNotIn(name, trace["env"])
        self.assertFalse((self.root / "session").exists())
        self.assertFalse((self.root / "verified").exists())

    def test_prepare_verifies_and_defers_log_scan(self):
        os.environ.update(RELEASE_VERSION="1.2.3", SONATYPE_TOKEN_PASSWORD="central-token")
        result = self.invoke("ci-release-prepare")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-DreleaseVersion=1.2.3", self.trace()["args"])
        self.assertIn("-DpushChanges=false", self.trace()["args"])
        self.assertNotIn("SONATYPE_TOKEN_PASSWORD", self.trace()["env"])
        self.assertTrue((self.root / "verified").exists())
        self.assertFalse(self.directory.exists())
        self.assertTrue(logs.raw_directory(self.directory).exists())

    def test_perform_verifies_history_before_requesting_private_credentials(self):
        os.environ["FAIL_VERIFY"] = "1"
        result = self.invoke("ci-release")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsigned history", result.stderr)
        self.assertFalse((self.root / "session").exists())
        self.assertFalse((self.root / "trace").exists())

    def test_dry_run_cannot_automatically_publish(self):
        result = self.invoke("ci-release-dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("-Drelease.autopublish=false", self.trace()["args"])
        self.assertTrue((self.root / "verified").exists())



    def test_broken_console_does_not_replace_maven_status(self):
        with patch("builtins.print", side_effect=BrokenPipeError), patch.object(os, "dup2"), redirect_stdout(StringIO()):
            self.assertEqual(signing.maven("validate", ["mvn", "verify"]), 0)
        logs.finish(self.directory)

    def test_completion_marker_failure_preserves_maven_result_and_withholds_logs(self):
        for status in (0, 17):
            os.environ["MAVEN_STATUS"] = str(status)
            with self.subTest(status=status), patch.object(Path, "touch", side_effect=OSError("disk full")), redirect_stdout(StringIO()):
                self.assertEqual(signing.maven("validate", ["mvn", "verify"]), status)
            logs.finish(self.directory)
            self.assertIn("withheld", (self.directory / "stdout.log").read_text())
            self.assertFalse(logs.raw_directory(self.directory).exists())

    def test_bad_version_is_rejected_before_signing_or_maven(self):
        os.environ["RELEASE_VERSION"] = "1.2.3 -Drelease.autopublish=true"
        result = self.invoke("ci-release-prepare")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("RELEASE_VERSION must", result.stderr)
        self.assertFalse((self.root / "session").exists())
        self.assertFalse((self.root / "trace").exists())

    def test_make_preview_runs_no_release_programs(self):
        for target in ("ci-release-prepare", "ci-release", "ci-release-dry-run"):
            result = self.invoke(target, "-n")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((self.root / "trace").exists())
            self.assertFalse((self.root / "session").exists())

    def test_local_publication_targets_are_removed(self):
        for target in ("release-prepare", "release", "release-dry-run", "_release",
                       "_release-prepare", "_release-dry-run", "_ci-release"):
            with self.subTest(target=target):
                result = self.invoke(target)
                self.assertNotEqual(result.returncode, 0)
        self.assertFalse((self.root / "trace").exists())
        self.assertFalse((self.root / "session").exists())

    def test_ci_publication_targets_reject_local_use(self):
        os.environ["GITHUB_ACTIONS"] = "false"
        for target in ("ci-release-prepare", "ci-release", "ci-release-dry-run"):
            with self.subTest(target=target):
                result = self.invoke(target)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("only by the protected CI workflow", result.stderr)
        self.assertFalse((self.root / "trace").exists())
        self.assertFalse((self.root / "session").exists())


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
        (source / ".mvn").mkdir()
        guard.check_config_files(source)

    def test_logs_inside_checkout_are_rejected_before_maven_clean(self):
        os.environ["RELEASE_LOG_DIR"] = str(ROOT / "target/release-logs")
        with self.assertRaisesRegex(signing.SigningError, "outside the source checkout"):
            signing.maven("prepare", ["mvn", "clean"])
        self.assertFalse((self.root / "session").exists())
        self.assertFalse((self.root / "trace").exists())


if __name__ == "__main__":
    unittest.main()
