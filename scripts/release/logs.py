"""Private file capture and an end-of-cycle, grep-style credential scan."""

from contextlib import contextmanager
import base64
import hashlib
import html
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import urllib.parse

LIMIT = 64 * 1024 * 1024
WITHHELD = b"Maven log withheld; see log-report.txt.\n"
CREDENTIAL = re.compile(
    rb"(?i)(?:authorization[\"']?\s*[:=]|"
    rb"(?:passphrase|password|token|secret)[\w.-]*[\"']?\s*[=:]\s*\S)"
)


def secret_forms(env):
    values = {value for name, value in env.items() if value and re.search(
        r"PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION", name, re.I)}
    for name in ("RELEASE_GPG_PRIVATE_KEY_FILE", "RELEASE_GPG_PASSPHRASE_FILE"):
        if env.get(name):
            values.add(Path(env[name]).read_text())
    if env.get("SONATYPE_TOKEN_USERNAME") and env.get("SONATYPE_TOKEN_PASSWORD"):
        values.add(env["SONATYPE_TOKEN_USERNAME"] + ":" + env["SONATYPE_TOKEN_PASSWORD"])
    values |= {line for value in values for line in value.splitlines() if line}
    forms = set(values)
    for value in values:
        forms.update((base64.b64encode(value.encode()).decode(),
                      base64.urlsafe_b64encode(value.encode()).decode(),
                      urllib.parse.quote(value, safe=""), urllib.parse.quote_plus(value),
                      html.escape(value), json.dumps(value)[1:-1],
                      json.dumps(value, ensure_ascii=False)[1:-1]))
    return {value.encode() for value in forms if value}


def raw_directory(logs):
    return logs.with_name(logs.name + "-private")


@contextmanager
def capture(logs, phase):
    raw = raw_directory(logs)
    raw.mkdir(mode=0o700, parents=True, exist_ok=True)
    if raw.is_symlink() or raw.stat().st_mode & 0o077:
        raise ValueError("release raw-log directory must be private")
    directory = Path(tempfile.mkdtemp(prefix=phase + "-", dir=raw))
    # No raw output is echoed or uploaded. Unmarked output is incomplete.
    def private_open(path):
        return open(path, "xb", opener=lambda name, flags: os.open(name, flags, 0o600))
    with private_open(directory / "stdout.log") as stdout, private_open(directory / "stderr.log") as stderr:
        yield directory, stdout, stderr


def report(logs, env):
    """Withhold suspect/incomplete logs, write cleared logs, then remove raw files."""
    raw = raw_directory(logs)
    if not raw.exists():
        return []
    # Failure here is handled by the caller: raw logs must never be uploaded.
    forms = secret_forms(env)
    messages = []
    remaining = LIMIT
    logs.mkdir(mode=0o700, parents=True, exist_ok=True)
    for directory in sorted(raw.iterdir(), key=lambda path: path.stat().st_mtime_ns):
        phase = directory.name.rsplit("-", 1)[0]
        if phase not in ("validate", "prepare", "perform", "dry-run") or directory.is_symlink():
            raise ValueError("unexpected raw-log entry")
        try:
            paths = [directory / (stream + ".log") for stream in ("stdout", "stderr")]
            size = sum(path.stat().st_size for path in paths)
            reason = None
            data = None
            if not (directory / "complete").exists():
                reason = "command interrupted; output may be incomplete"
            elif size > remaining:
                reason = "cycle scan limit exceeded"
            else:
                remaining -= size
                data = []
                budget = size
                for path in paths:
                    with path.open("rb") as source:
                        content = source.read(budget + 1)
                    if len(content) > budget:
                        reason = "log changed during scan"
                        break
                    budget -= len(content)
                    data.append(content)
                if not reason and any(CREDENTIAL.search(stream) or any(value in stream for value in forms)
                                      for stream in data):
                    reason = "possible credential output"
            if reason:
                data = [WITHHELD, WITHHELD]
                messages.append(f"{phase}: {reason}; both logs withheld.")
            else:
                messages.append(f"{phase}: logs cleared by credential scan.")
            for stream, content in zip(("stdout", "stderr"), data):
                with (logs / (stream + ".log")).open("ab") as output:
                    output.write(f"--- {phase} ---\n".encode() + content)
        finally:
            shutil.rmtree(directory)
    raw.rmdir()
    return messages


def cleanup(logs):
    raw = raw_directory(logs)
    if raw.is_dir() and not raw.is_symlink():
        for directory in raw.iterdir():
            if (directory.name.rsplit("-", 1)[0] in ("validate", "prepare", "perform", "dry-run")
                    and directory.is_dir() and not directory.is_symlink()):
                shutil.rmtree(directory)
        raw.rmdir()


def notice(message):
    try:
        print(message, flush=True)
    except OSError:
        # Avoid a failed interpreter-shutdown flush replacing Maven's status.
        try:
            with open(os.devnull, "w") as sink:
                os.dup2(sink.fileno(), sys.stdout.fileno())
        except OSError:
            pass


def finish(directory):
    # Reporting never changes a completed Maven result or prevents the SCM push.
    messages = []
    try:
        messages = report(directory, os.environ)
    except Exception:
        messages = ["Maven log processing failed; raw output withheld. Check publication state before retrying."]
    finally:
        try:
            cleanup(directory)
        except OSError:
            messages.append("Raw Maven log cleanup failed; remove the private capture directory after recovery.")
    for message in messages:
        prefix = "::warning::" if os.environ.get("GITHUB_ACTIONS") == "true" and "cleared" not in message else ""
        notice(prefix + message)
    try:
        if messages:
            directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            with (directory / "log-report.txt").open("a") as output:
                output.write("\n".join(messages) + "\n")
        if messages and os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
                summary.write("\n### Maven log report\n\n")
                summary.write("Log findings do not change the release outcome; inspect the Maven and SCM steps before retrying.\n\n")
                summary.write("\n".join("- " + message for message in messages) + "\n")
    except OSError:
        pass


def log_directory():
    # Maven clean and SCM preparation must never touch these files.
    checkout = hashlib.sha256(os.fsencode(Path.cwd())).hexdigest()[:12]
    default = Path(tempfile.gettempdir()) / f"cdc-release-{os.getuid()}-{checkout}"
    return Path(os.environ.get("RELEASE_LOG_DIR", str(default))).absolute()


if __name__ == "__main__":
    if sys.argv[1:] != ["report"]:
        sys.exit("Expected report")
    finish(log_directory())
