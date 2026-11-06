"""Run every blocker gate against paginated issues/PRs and API failures."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from workflow_helpers import workflow_source

ROOT = Path(__file__).resolve().parents[2]


class BlockerTests(unittest.TestCase):
    def test_all_gates_preserve_trusted_tooling_and_fail_closed(self):
        commands = []
        for workflow in ("release", "release-dry-run"):
            source = workflow_source(workflow)
            gates = [line.strip().removeprefix("run: ") for line in source.splitlines()
                     if "run: bash " in line and "check-blockers.sh" in line]
            self.assertEqual(len(gates), 4)
            commands.extend(gates)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            preserved = root / "release-tooling"
            preserved.mkdir()
            shutil.copy2(ROOT / "scripts/release/check-blockers.sh", preserved)
            target = root / "historical-target"
            (target / "scripts/release").mkdir(parents=True)
            (target / "scripts/release/check-blockers.sh").write_text("exit 99\n")
            gh = root / "gh"
            gh.write_text(f"#!{sys.executable}\n" +
                "import os,sys\n"
                "assert sys.argv[1:] == ['api','--paginate','--slurp',"
                "'repos/fixture/java/issues?state=open&labels=release-blocker&per_page=100']\n"
                "print(os.environ['BLOCKER_RESULT'])\n"
                "sys.exit(int(os.environ['BLOCKER_STATUS']))\n")
            gh.chmod(0o700)
            issue = {"html_url": "https://example.invalid/issues/1"}
            pr = {"html_url": "https://example.invalid/pull/2", "pull_request": {}}
            for command in commands:
                for output, status, expected in (("[[]]", 0, {0}), (json.dumps([[issue]]), 0, {1}),
                        (json.dumps([[pr]]), 0, {1}), (json.dumps([[issue] * 100, [pr]]), 0, {1}),
                        ("", 7, {7}), ("invalid JSON", 0, {4, 5})):
                    with self.subTest(command=command, status=status, output=output):
                        result = subprocess.run(["bash", "-eo", "pipefail", "-c", command],
                            cwd=ROOT if "bash scripts/" in command else target,
                            env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                "RUNNER_TEMP": str(root), "GITHUB_REPOSITORY": "fixture/java",
                                "BLOCKER_RESULT": output, "BLOCKER_STATUS": str(status)},
                            capture_output=True, text=True, timeout=10)
                        self.assertIn(result.returncode, expected, result.stderr)
                        self.assertEqual("No open release blockers found." in result.stdout, expected == {0})
                        if expected == {1}:
                            self.assertIn("::error title=Release blocked::", result.stdout)
                            for page in json.loads(output):
                                for item in page: self.assertIn(item["html_url"], result.stdout)


if __name__ == "__main__":
    unittest.main()
