#!/usr/bin/env python3
"""Run fixed Maven release phases in one signing/execution owner."""

import hashlib
import os
from pathlib import Path
import re
import sys
import tempfile

import guard
import logs
import signing


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


def check_user(ci=False):
    if ci:
        if os.environ.get("GITHUB_ACTIONS") != "true":
            raise guard.Unsafe("Use local release targets outside GitHub Actions")
    else:
        try:
            user = signing.run(["gh", "api", "user", "--jq", ".login"]).strip()
        except signing.SigningError:
            user = None
        if user != "scylladb-publisher":
            raise guard.Unsafe("Release requires GitHub user scylladb-publisher; run gh auth login or gh auth switch")


def maven_command(phase, env):
    command = ["mvn", "-B", "--no-transfer-progress"]
    skip = env.get("RELEASE_SKIP_TESTS", "false")
    if skip not in ("true", "false", "1", "0"):
        raise guard.Unsafe("RELEASE_SKIP_TESTS must be a Boolean")
    if skip in ("true", "1"):
        env["MAVEN_OPTS"] = "-DskipTests=true -DskipITs=true"
    if phase == "validate":
        return command + ["-Prelease", "-Dgpg.skip=true", "verify"]
    if phase == "prepare":
        command += ["clean", "release:clean", "release:prepare", "-DpushChanges=false"]
        version = env.get("RELEASE_VERSION", "")
        if version:
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.+-]*", version):
                raise guard.Unsafe("RELEASE_VERSION must contain only version characters")
            command.append("-DreleaseVersion=" + version)
        return command
    return command + ["release:perform", "-Drelease.autopublish=" + str(phase == "perform").lower()]


def run_phase(phase, directory):
    if directory.resolve().is_relative_to(Path.cwd().resolve()):
        raise guard.Unsafe("RELEASE_LOG_DIR must be outside the source checkout")
    guard.check_policy(Path.cwd())
    env = guard.maven_environment(os.environ)
    if phase in ("validate", "prepare"):
        for name in ("SONATYPE_TOKEN_USERNAME", "SONATYPE_TOKEN_PASSWORD"):
            env.pop(name, None)
    if phase == "validate":
        env = {name: value for name, value in env.items()
               if not name.startswith("RELEASE_GPG_")
               and name not in ("MAVEN_GPG_PASSPHRASE", "PGP_PASSPHRASE")}
    command = maven_command(phase, env)
    if phase in ("perform", "dry-run"):
        signing.verify_prepared()
    baseline = signing.run(["git", "rev-parse", "HEAD"]).strip() if phase == "prepare" else None
    with logs.capture(directory, phase) as (raw, stdout, stderr):
        if phase == "validate":
            status = signing.execute(command, env, stdout=stdout, stderr=stderr)
        else:
            with signing.signing_session(env) as session:
                status = signing.execute(command, session, stdout=stdout, stderr=stderr)
                if status == 0 and phase == "prepare":
                    signing.verify_prepared(baseline)
        try:
            (raw / "complete").touch(mode=0o600, exist_ok=False)
        except OSError:
            # The report withholds unmarked output; preserve Maven's result.
            pass
    notice(f"Maven {phase} exited with status {status}; logs will be scanned before upload.")
    return status


def report(directory):
    # Reporting never changes a completed Maven result or prevents the SCM push.
    messages = []
    try:
        messages = logs.report(directory, os.environ)
    except Exception:
        messages = ["Maven log processing failed; raw output withheld. Check publication state before retrying."]
    finally:
        try:
            logs.cleanup(directory)
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


def main():
    # Keep logs outside the checkout: Maven clean deletes target/, and release
    # preparation checks for uncommitted files. Separate local checkouts too.
    checkout = hashlib.sha256(os.fsencode(Path.cwd())).hexdigest()[:12]
    default = Path(tempfile.gettempdir()) / f"cdc-release-{os.getuid()}-{checkout}"
    directory = Path(os.environ.get("RELEASE_LOG_DIR", str(default))).absolute()
    args = sys.argv[1:]
    phase = args[0] if args else ""
    defer = "--defer-logs" in args[1:]
    try:
        if phase == "report" and len(args) == 1:
            report(directory)
            return 0
        if phase in ("check-user", "check-ci") and len(args) == 1:
            check_user(phase == "check-ci")
            return 0
        if (phase not in ("validate", "prepare", "perform", "dry-run")
                or any(arg not in ("--ci", "--defer-logs") for arg in args[1:])):
            raise guard.Unsafe("expected validate, prepare, perform, dry-run, or report")
        if phase != "validate":
            check_user("--ci" in args[1:])
        return run_phase(phase, directory)
    except (guard.Unsafe, signing.SigningError) as error:
        print(f"Release stopped: {error}.", file=sys.stderr)
        return 2
    except Exception:
        print("Release operation failed; values withheld.", file=sys.stderr)
        return 2
    finally:
        if phase in ("validate", "prepare", "perform", "dry-run") and not defer:
            report(directory)


if __name__ == "__main__":
    sys.exit(main())
