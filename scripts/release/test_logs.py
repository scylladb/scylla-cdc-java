"""Exercise private file capture, post-run scanning, and safe reporting failures."""

from contextlib import redirect_stdout
from io import StringIO
import base64
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

import logs


class LogTests(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory()
        self.addCleanup(fixture.cleanup)
        self.directory = Path(fixture.name) / "logs"

    def capture(self, stdout=b"normal output\n", stderr=b"", completed=True, phase="prepare"):
        with logs.capture(self.directory, phase) as (raw, out, err):
            self.assertEqual(stat.S_IMODE(raw.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE((raw / "stdout.log").stat().st_mode), 0o600)
            out.write(stdout)
            err.write(stderr)
            if completed:
                (raw / "complete").touch()
        return raw

    def test_raw_output_is_private_until_end_of_cycle(self):
        self.capture(phase="prepare")
        self.capture(b"published\n", phase="perform")
        self.assertFalse(self.directory.exists())
        messages = logs.report(self.directory, {})
        self.assertEqual(len(messages), 2)
        self.assertTrue(all("cleared" in message for message in messages))
        self.assertIn(b"published", (self.directory / "stdout.log").read_bytes())
        self.assertFalse(logs.raw_directory(self.directory).exists())

    def test_credential_match_withholds_both_streams_without_echoing_value(self):
        secret = "synthetic-publisher-password"
        for value in (secret.encode(), base64.b64encode(secret.encode()), b"Authorization: Basic arbitrary"):
            with self.subTest(value=value):
                self.capture(stderr=value)
                messages = logs.report(self.directory, {"SONATYPE_TOKEN_PASSWORD": secret})
                self.assertIn("possible credential", messages[0])
                for path in self.directory.iterdir():
                    self.assertNotIn(value, path.read_bytes())
                    self.assertNotIn(secret.encode(), path.read_bytes())

    def test_encoded_passphrase_output_is_scanned(self):
        secret = "sëcret&phrase"
        self.capture(stdout="sëcret&amp;phrase".encode())
        # HTML and JSON forms, as well as literal values, use fixed-string matching.
        self.capture(stdout=b"s\\u00ebcret&phrase")
        messages = logs.report(self.directory, {"MAVEN_GPG_PASSPHRASE": secret})
        self.assertTrue(all("possible credential" in message for message in messages))

    def test_incomplete_and_oversized_logs_are_withheld(self):
        self.capture(b"incomplete sensitive fragment", completed=False)
        self.capture(b"too large", phase="perform")
        with patch.object(logs, "LIMIT", 3):
            messages = logs.report(self.directory, {})
        self.assertTrue(any("incomplete" in message for message in messages))
        self.assertTrue(any("limit" in message for message in messages))
        self.assertNotIn(b"fragment", (self.directory / "stdout.log").read_bytes())
        self.assertFalse(logs.raw_directory(self.directory).exists())

    def test_scan_failure_withholds_output_and_cleans_raw_files(self):
        self.capture(b"never-upload-this")
        summary = self.directory.parent / "summary"
        with patch.object(logs, "secret_forms", side_effect=OSError("sensitive-error")), \
                patch.dict(os.environ, GITHUB_ACTIONS="true", GITHUB_STEP_SUMMARY=str(summary)), \
                redirect_stdout(StringIO()) as output:
            logs.finish(self.directory)
        self.assertIn("::warning::", output.getvalue())
        self.assertNotIn("sensitive-error", output.getvalue())
        self.assertIn("do not change the release outcome", summary.read_text())
        self.assertFalse(logs.raw_directory(self.directory).exists())
        self.assertEqual({path.name for path in self.directory.iterdir()}, {"log-report.txt"})


if __name__ == "__main__":
    unittest.main()
