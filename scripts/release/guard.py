#!/usr/bin/env python3
"""Check release defaults and capture Maven output without writing raw credentials.

Output is buffered until Maven exits (64 MiB maximum), then sanitized for both the
console and log artifacts. Overflow and incomplete streams produce fixed diagnostics.
Child Maven processes run normally; they are not intercepted.
"""

import base64
import heapq
import html
import json
import os
from pathlib import Path
import re
import selectors
import signal
import shlex
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET

LIMIT = 64 * 1024 * 1024
DRAIN_GRACE = 1
OVERFLOW_LOG = b"Maven output exceeded the capture limit; invocation output withheld.\n"
INCOMPLETE_LOG = b"Maven output stream did not close; incomplete output withheld.\n"
CANCELLED_LOG = b"Maven was interrupted; incomplete output withheld.\n"
FAILED_LOG = b"Sanitized Maven output capture failed; invocation output withheld.\n"
ENV_OPTIONS = (
    "MAVEN_ARGS",
    "MAVEN_OPTS",
    "JAVA_TOOL_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS",
    "MAVEN_DEBUG_OPTS",
    "MAVEN_CONFIG",
)


CHILD_OPTIONS = r"arguments|prepareVerifyArgs|goals|preparationGoals|completionGoals"


class Unsafe(ValueError):
    pass


def check_text(text, source=None, maven_options=True, maven_arguments=(), jvm_options=False):
    # Maven accepts an equals sign after -D/--D; treat it as the ordinary
    # property form before every safety check.
    def normalize_property(value):
        value = re.sub(r"(?<![\w-])--?D=", "-D", value, flags=re.I)
        value = re.sub(r"(?i)(?<![\w-])(--?define=)\s+", r"\1", value)
        value = re.sub(
            r"(?i)(?<![\w-])((?:--?D\s*|--?define(?:=|\s+)))([\"'])([\w.-]+)\s+=",
            r"\1\2\3=", value,
        )
        return re.sub(
            r"(?i)(?<![\w-])((?:--?D\s*|--?define(?:=|\s+))[\w.-]+)\s+=",
            r"\1=", value,
        )

    def normalize_outer_property(value):
        return re.sub(
            r"(?i)^(--?D\s*|--?define(?:=|\s+))([\"'])(.*)\2$",
            lambda match: match[1] + match[3], normalize_property(value),
        )

    text = normalize_property(text)
    # Maven removes quotes before interpreting property names and values.
    original_maven_arguments = tuple(maven_arguments)
    maven_arguments = tuple(
        normalize_property(argument.translate(str.maketrans("", "", "\"'")))
        for argument in original_maven_arguments
    )
    if re.search(r"(?<![\w-])-[A-Za-z](?:=\s*)?-(?=[A-Za-z-])", text):
        raise Unsafe("combined Maven options are not allowed")
    def check_autopublish(argument):
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))release\.autopublish=(.*)",
            argument, re.I | re.S,
        )
        if setting and setting[1] not in ("true", "false", "${release.autopublish}"):
            raise Unsafe("release.autopublish must be a Boolean")

    try:
        arguments = [argument.strip().strip("\"'") for argument in maven_arguments]
        jvm_arguments = shlex.split(text) if jvm_options else []
    except ValueError as error:
        raise Unsafe("malformed release options are not allowed") from error
    if any(re.match(r"-XX:VMOptionsFile(?:=|$)", argument) for argument in jvm_arguments):
        raise Unsafe("unverified JVM option files are not allowed")
    def check_pom_option(argument):
        # Maven can split an equals-separated option into a second flag.
        # Keep property values such as -Dsafe=-f intact.
        if (re.match(r"^--?[A-Za-z][\w-]*=--?[A-Za-z]", argument)
            and not re.match(r"^--?D[\w.-]*=|^--?define=", argument, re.I)):
            raise Unsafe("combined Maven options are not allowed")
        if (argument.startswith("-f") and argument not in ("-fae", "-ff", "-fn")) or (
            argument.startswith("--f")
            and argument not in ("--fail-at-end", "--fail-fast", "--fail-never")
        ):
            raise Unsafe("alternate Maven POMs are not allowed")
        if (argument.startswith(("-s", "-gs")) and not argument.startswith("--")) or (
            argument in ("--s", "--gs", "--settings", "--global-settings", "-global-settings")
            or argument.startswith(("--s=", "--gs=", "--settings=", "--global-settings=",
                                    "-global-settings="))
        ):
            raise Unsafe("alternate Maven settings are not allowed")

    def check_release_arguments(value):
        if "${" in value:
            raise Unsafe("release arguments must not contain unverified properties")
        if "\\" in value:
            raise Unsafe("backslash escapes in release arguments are not allowed")
        try:
            lexer = shlex.shlex(value, posix=True)
            lexer.whitespace_split = True
            lexer.escape = ""  # Maven's child tokenizer keeps backslashes literal.
            lexer.commenters = ""  # Maven does not treat # as a comment here.
            child_arguments = list(lexer)
        except ValueError as error:
            raise Unsafe("malformed release options are not allowed") from error
        for child_argument in child_arguments:
            check_pom_option(child_argument)
        check_text(value, maven_arguments=child_arguments)

    for argument in arguments:
        if argument.startswith(("-X", "--X")):
            raise Unsafe("Maven debug options are not allowed")
        check_pom_option(argument)
    # Release-plugin properties are passed on to child Maven invocations.
    option_arguments = arguments + jvm_arguments
    for index, argument in enumerate(option_arguments):
        if index and option_arguments[index - 1] in ("-D", "--D", "-define", "--define"):
            argument = normalize_property(option_arguments[index - 1] + " " + argument)
        check_autopublish(argument)
        setting = re.fullmatch(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=(.*)",
            argument, re.I | re.S,
        )
        if setting and index >= len(arguments):
            check_release_arguments(setting[1])
    if not option_arguments:
        for setting in re.finditer(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=([^\r\n]*)",
            text, re.I,
        ):
            check_release_arguments(setting[1])
    if any(argument.startswith("@") for argument in arguments + jvm_arguments):
        raise Unsafe("unverified argument files are not allowed")
    if jvm_options and re.search(r"-Dgpg\.[^\s]*bestPractices[^\s]*[\"']", text, re.I):
        raise Unsafe("GPG best practices must not be disabled")
    for argument in arguments + jvm_arguments:
        check_autopublish(argument)
    for index, argument in enumerate(maven_arguments):
        previous = maven_arguments[index - 1].strip().strip("\"'") if index else ""
        raw = original_maven_arguments[index].strip()
        if len(raw) > 1 and raw[0] in "\"'" and raw[-1] == raw[0]:
            raw = raw[1:-1]
        if previous in ("-D", "--D", "-define", "--define"):
            argument = normalize_property(previous + " " + argument.strip("\"'"))
        if re.fullmatch(r"(?:--?D\s*|--?define(?:=|\s+))release\.autopublish=(?:true|false)",
                        argument, re.I | re.S):
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            raw_option = normalize_outer_property(raw_option)
            raw_option = re.sub(
                r"(?i)^(--?D\s*|--?define(?:=|\s+))([\"'])(.*)\2$",
                lambda match: match[1] + match[3], raw_option,
            )
            raw_value = raw_option.rsplit("=", 1)[-1]
            if raw_value.lower() not in ("true", "false"):
                raise Unsafe("release.autopublish must be a Boolean")
        # Quote deletion is useful for finding quoted property names, but it
        # must not turn a literal value such as tr'ue into a valid Boolean.
        if re.fullmatch(r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=true",
                        argument, re.I | re.S):
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            value = normalize_outer_property(raw_option).rsplit("=", 1)[-1]
            if value.lower() != "true":
                raise Unsafe("GPG best practices must not be disabled")
        check_autopublish(argument)
        setting = re.fullmatch(
            r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(.*)", argument, re.I | re.S
        )
        if setting and setting[1].lower() != "true":
            raise Unsafe("GPG best practices must not be disabled")
        child_setting = re.fullmatch(
            rf"(?:--?D\s*|--?define(?:=|\s+))(?:{CHILD_OPTIONS})=(.*)",
            argument, re.I | re.S,
        )
        if child_setting:
            raw_option = (previous + " " if previous in ("-D", "--D", "-define", "--define")
                          else "") + raw
            raw_option = normalize_outer_property(raw_option)
            raw_setting = re.fullmatch(
                rf"(?:--?D\s*|--?define(?:=|\s+))[\"']?(?:{CHILD_OPTIONS})[\"']?=(.*)",
                raw_option, re.I | re.S,
            )
            if not raw_setting:
                raise Unsafe("malformed release options are not allowed")
            check_release_arguments(raw_setting[1])
    # Preserve whitespace inside quoted GPG values when checking the full boolean.
    gpg_text = re.sub(
        r"([\"'])((?:\\.|(?!\1)[\s\S])*)\1",
        lambda match: re.sub(r"\s", "\0", match[2]), text,
    ).translate(str.maketrans("", "", "\"'"))
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))gpg\.bestPractices=(?!true(?:\s|$))",
                 gpg_text, re.I) or (jvm_options and re.search(
                     r"(?:^|\s)-Dgpg\.bestPractices(?:\s|$)", gpg_text, re.I)):
        raise Unsafe("GPG best practices must not be disabled")
    # Java and Maven option parsing remove quotes from property names and values.
    text = normalize_property(text.translate(str.maketrans("", "", "\"'")))
    if re.search(r"(?:--?D\s*|--?define(?:=|\s+))pomFileName\b", text, re.I):
        raise Unsafe("alternate Maven POMs are not allowed")
    if maven_options and re.search(
        r"(?<![\w-])(?:--?s|--?gs|--settings|--?global-settings)(?=\s|=|$)", text
    ):
        raise Unsafe("alternate Maven settings are not allowed")
    # Catch accidental debug flags and credential properties, including release arguments.
    match = re.search(
        r"(?:^|[\s=\"'])(?:(?-i:--?X)|--?debug)\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))(?:arguments|prepareVerifyArgs)="
        r"(?:-X|[^\r\n]*?[=\s\"']-X)"
        r"|(?:--?D\s*|--?define(?:=|\s+))javax\.net\.debug\b"
        r"|(?:--?D\s*|--?define(?:=|\s+))"
        r"(?:user\.dir|file\.encoding|sun\.std(?:out|err)\.encoding)\b"
        # These properties can redirect Maven to unchecked configuration.
        r"|(?:--?D\s*|--?define(?:=|\s+))"
        r"(?:maven\.multiModuleProjectDirectory|maven\.(?:conf|home)|user\.home)\b"
        r"|simpleLogger\.logFile\b"
        r"|simpleLogger[^\s<>]*=(?:debug|trace)\b"
        r"|simpleLogger[^\s<>]*=[^\s<>]*\$\{"
        r"|(?:--?D\s*|--?define(?:=|\s+))[\w.-]*"
        r"(?:passphrase|password|token|secret|credentials?|authorization)[\w.-]*="
        # Inspect interpolation sources, including ordinary property aliases.
        # An environment name is safe; its interpolated credential value is not.
        r"|\$\{env\.[^}]*?(?:PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION|\$\{)"
        # Maven context objects allow reflective/map access. Accept only complete
        # public scalar project metadata; keep shell/GHA placeholders distinct.
        r"|(?-i:\$\{(?![A-Za-z_]\w*:(?:[-+?=][\w/-]*|[0-9]+(?::[0-9]+)?)\})"
        r"(?:session|settings|plugin|mojo))"
        r"|(?-i:\$\{(?:localRepository|repositorySystemSession|reactorProjects|executedProject|mojoExecution)"
        r"(?:[.(\[]|\}))"
        r"|(?-i:\$\{(?![A-Za-z_]\w*:(?:[-+?=][\w/-]*|[0-9]+(?::[0-9]+)?)\})"
        r"(?:project|pom)(?!\.(?:parent\.)*"
        r"(?:version|groupId|artifactId|packaging|name|description|url|basedir|"
        r"build\.(?:directory|outputDirectory|testOutputDirectory|sourceDirectory|testSourceDirectory|finalName|sourceEncoding)|"
        r"reporting\.outputDirectory)\}))"
        # Quotes were removed above; '=' also starts a property alias's value.
        r"|(?:^|[\s=])--(?:pass|passp|passph|passphr|passphra|passphras|passphrase)"
        r"(?:[-=\s]|$)",
        text,
        re.I,
    )
    if maven_options and not match:
        match = re.search(
            r"(?:^|[\s=\"'])(?:--(?:l|lo|log|log-|log-f|log-fi|log-fil|log-file)(?=[=\s]|$)"
            r"|-l(?!lr(?:[\s\"']|$)))", text
        )
    if match:
        if source is not None:
            start = match.start() + len(match[0]) - len(match[0].lstrip())
            line = text.count("\n", 0, start) + 1
            print(f"Unsafe release options at {source}:{line}; values withheld.", file=sys.stderr)
        raise Unsafe("debug logging or credential arguments are not allowed")


def check_policy(root):
    root = root.resolve()
    pom = ET.parse(root / "pom.xml").getroot()
    # Maven also accepts POM XML without the standard namespace.
    if pom.findtext("{*}properties/{*}gpg.bestPractices") != "true":
        raise Unsafe("enable GPG best practices before releasing this target")
    # Inspect child and profile POMs too: inherited defaults can be overridden.
    paths = [path for path in sorted(root.rglob("pom.xml"))
             if not any(part in ("target", ".git") for part in path.relative_to(root).parts)]
    seen = set()
    while paths:
        path = paths.pop().resolve()
        if root not in path.parents:
            raise Unsafe("reactor POMs must remain within the selected source")
        if path in seen:
            continue
        seen.add(path)
        if not path.is_file():
            raise Unsafe("reactor POM cannot be verified")
        try:
            pom = ET.parse(path).getroot()
        except (OSError, ET.ParseError):
            raise Unsafe("reactor POM cannot be verified") from None
        # Maven modules may name a POM file instead of a directory, including in
        # profiles. Follow literal paths without trying to resolve Maven models.
        # Plugin configuration may also contain unrelated <modules> elements.
        modules = (pom.findall("{*}modules/{*}module")
                   + pom.findall("{*}profiles/{*}profile/{*}modules/{*}module"))
        for module in modules:
            name = (module.text or "").strip()
            if not name or "${" in name:
                raise Unsafe("reactor module paths must be literal")
            child = (path.parent / name).resolve()
            if child != root and root not in child.parents:
                raise Unsafe("reactor POMs must remain within the selected source")
            if child.is_dir():
                child /= "pom.xml"
            # No POM can execute here if absent; mandatory Maven validation
            # rejects missing modules. Existing files must always be inspected.
            if child.exists():
                paths.append(child)
        for plugin in pom.findall(".//{*}plugin"):
            if plugin.findtext("{*}artifactId") == "maven-gpg-plugin":
                for setting in plugin.findall(".//{*}bestPractices"):
                    if (setting.text or "").strip().lower() != "true":
                        raise Unsafe("GPG best practices must not be disabled")
            if plugin.findtext("{*}artifactId") == "central-publishing-maven-plugin":
                version = plugin.findtext("{*}version")
                if version and (
                    not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version)
                    or tuple(map(int, version.split("."))) < (0, 11, 0)
                ):
                    raise Unsafe("update Central publishing before releasing this target")
        for element in pom.iter():
            if (element.tag.rsplit("}", 1)[-1] == "gpg.bestPractices"
                    and (element.text or "").strip().lower() != "true"):
                raise Unsafe("GPG best practices must not be disabled")
            if element.tag.rsplit("}", 1)[-1].lower() in (
                    "passphrase", "password", "gpg.passphrase"):
                raise Unsafe("POM credential configuration is not allowed")
            check_text(element.text or "",
                       f"{path.relative_to(root)} <{element.tag.rsplit('}', 1)[-1]}>")
    check_config_files(root)


def check_config_files(root):
    for name in ("maven.config", "jvm.config"):
        path = root / ".mvn" / name
        if path.exists():
            text = path.read_text()
            check_text(
                text, path.relative_to(root),
                maven_arguments=text.splitlines() if name == "maven.config" else (),
                jvm_options=name == "jvm.config",
            )


def secret_forms(env):
    values = {
        v
        for k, v in env.items()
        if v and re.search(r"PASSWORD|PASSPHRASE|TOKEN|SECRET|PRIVATE_KEY|AUTHORIZATION", k, re.I)
    }
    user, password = env.get("SONATYPE_TOKEN_USERNAME"), env.get("SONATYPE_TOKEN_PASSWORD")
    if user and password:
        values.add(user + ":" + password)
    # Include multiline components, escaped newlines, URL and both Base64 alphabets.
    values |= {line for value in values for line in value.splitlines() if line}
    values |= {value.replace("\n", "\\n").replace("\r", "\\r") for value in values}
    # JSON/logging serializers also escape quotes, backslashes and Unicode secrets.
    values |= {json.dumps(value, ensure_ascii=ascii_only)[1:-1]
               for value in tuple(values) for ascii_only in (True, False)}
    # Maven XML serializers escape punctuation and may use named or numeric quotes.
    for value in tuple(values):
        escaped = html.escape(value, quote=False)
        for apostrophe in ("'", "&apos;", "&#39;", "&#x27;"):
            for quote in ('"', "&quot;", "&#34;", "&#x22;"):
                values.add(escaped.replace("'", apostrophe).replace('"', quote))
    forms = set(values)
    for value in values:
        raw = value.encode()
        forms.update(
            (
                base64.b64encode(raw).decode(),
                base64.urlsafe_b64encode(raw).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote_plus(value, safe=""),
            )
        )
    for value in tuple(forms):
        forms.add(urllib.parse.quote(value, safe=""))
        forms.add(urllib.parse.quote(value))
        forms.add(value.rstrip("="))
    return sorted((v.encode() for v in forms if v), key=len, reverse=True)


def redact(data, forms):
    # Match every source against the original bytes so replacements cannot hide secrets.
    def spans(pattern, group, flags=0):
        return (match.span(group) for match in re.finditer(pattern, data, flags))

    matches = [
        spans(
            rb"(?i)([\"'])((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"((?:\\[\s\S]|(?!\1)[^\\])*)\1", 3,
        ),
        spans(
            rb"(?i)((?:-D\s*|--?define[=\s]+)[\w.-]*(?:passphrase|password|username|token|secret|authorization)[\w.-]*=)"
            rb"(\"(?:\\[\s\S]|[^\"\\])*\"|'(?:\\[\s\S]|[^'\\])*'|[^\s]+)", 2,
        ),
        spans(rb"(?im)((?:proxy-)?authorization[\"']?\s*[:=]\s*)([^\r\n]*)", 2),
    ]
    for value in forms:
        # Lookahead includes overlapping occurrences; percent escapes ignore case.
        matches.append(spans(b"(?=(" + re.escape(value) + b"))", 1, re.I if b"%" in value else 0))
    output = bytearray()
    start = end = 0
    for left, right in heapq.merge(*matches):
        if left > end:
            if end > start:
                output.extend(b"[REDACTED]")
            output.extend(data[end:left])
            start = left
        end = max(end, right)
    if end > start:
        output.extend(b"[REDACTED]")
    output.extend(data[end:])
    return bytes(output)


def write_logs(clean, logs):
    """Write only sanitized content or fixed diagnostics, tolerating broken outputs."""
    output_failed = False
    for name, data, stream in zip(("stdout", "stderr"), clean, (sys.stdout, sys.stderr)):
        try:
            logs.mkdir(parents=True, exist_ok=True)
            with (logs / (name + ".log")).open("ab") as output:
                output.write(data)
        except OSError:
            output_failed = True
        try:
            stream.buffer.write(data)
            stream.flush()
        except OSError:
            output_failed = True
            # Prevent a failed shutdown flush from replacing Maven's exit status.
            try:
                with open(os.devnull, "wb") as sink:
                    os.dup2(sink.fileno(), stream.fileno())
            except OSError:
                # Redirection is best effort; preserve Maven's original exit status.
                pass
    if output_failed:
        try:
            os.write(2, b"Sanitized release output could not be fully written; values withheld.\n")
        except OSError:
            # Stderr is already unusable, so no further diagnostic can be emitted.
            pass


def capture(command, env, forms, logs):
    """Never spool raw output or sanitize a stream cut off before EOF."""
    process = None
    buffers = [bytearray(), bytearray()]
    complete = [False, False]
    overflow = False
    logs_started = False
    # Defer parent cancellation until ownership; the child keeps its prior mask.
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, (signal.SIGTERM, signal.SIGHUP, signal.SIGINT))
    try:
        process = subprocess.Popen(
            command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
            preexec_fn=lambda: signal.pthread_sigmask(signal.SIG_SETMASK, previous),
        )
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            exited_at = None
            while selector.get_map():
                # A successful Maven parent can leave pipe-connected publication
                # work running. Drain it to EOF before allowing the SCM push.
                if process.poll() not in (None, 0):
                    if exited_at is None:
                        exited_at = time.monotonic()
                    elif time.monotonic() - exited_at >= DRAIN_GRACE:
                        break
                for key, _ in selector.select(timeout=0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        complete[key.data] = True
                        selector.unregister(key.fileobj)
                    elif not overflow:
                        if sum(map(len, buffers)) + len(chunk) > LIMIT:
                            overflow = True
                            buffers = [bytearray(), bytearray()]
                            continue
                        buffers[key.data].extend(chunk)
        status = process.wait()
        # Sanitize both streams completely before opening artifacts or emitting output.
        try:
            clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                redact(bytes(buffer), forms) if ended else INCOMPLETE_LOG
                for buffer, ended in zip(buffers, complete)
            ]
        except Exception:
            # Publishing has already exited. Withhold output without changing its
            # status or skipping the workflow's successful SCM push.
            clean = [FAILED_LOG, FAILED_LOG]
        logs_started = True
        write_logs(clean, logs)
        return status if status >= 0 else 128 - status
    except BaseException as error:
        if process is None:
            raise
        completed_status = process.poll()
        signum = signal.SIGTERM
        if isinstance(error, SystemExit) and isinstance(error.code, int):
            signum = error.code - 128
        if signum not in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            signum = signal.SIGTERM
        try:
            os.killpg(process.pid, signum)
        except ProcessLookupError:
            # The process group may already have exited before cancellation.
            pass
        try:
            process.wait(timeout=DRAIN_GRACE)
        except subprocess.TimeoutExpired:
            # The grace period expired; force termination of the group below.
            pass
        # The Maven parent may exit before a descendant responds to cancellation.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        clean = [FAILED_LOG, FAILED_LOG]
        if isinstance(error, (SystemExit, KeyboardInterrupt)):
            try:
                clean = [OVERFLOW_LOG, OVERFLOW_LOG] if overflow else [
                    redact(bytes(buffer), forms) if ended else CANCELLED_LOG
                    for buffer, ended in zip(buffers, complete)
                ]
            except Exception:
                # Keep FAILED_LOG if cancellation-time redaction also fails.
                pass
        if not logs_started:
            write_logs(clean, logs)
        if isinstance(error, Exception) and completed_status is not None:
            return completed_status if completed_status >= 0 else 128 - completed_status
        raise
    finally:
        if process is not None:
            process.stdout.close()
            process.stderr.close()
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def interrupted(signum, frame):
    raise SystemExit(128 + signum)


def check_runtime_options():
    for name in ENV_OPTIONS:
        check_text(
            os.environ.get(name, ""),
            maven_arguments=os.environ.get(name, "").split() if name == "MAVEN_ARGS" else (),
            jvm_options=name not in ("MAVEN_ARGS", "MAVEN_CONFIG"),
        )


def main():
    try:
        if sys.argv[1:] == ["check"]:
            check_runtime_options()
            check_policy(Path.cwd())
            print("Release credential safety checks passed.")
            return 0
        if len(sys.argv) < 3 or sys.argv[1] != "run" or not sys.argv[2]:
            raise Unsafe("expected check or run Maven executable ...")
        if any(arg.startswith("-X") for arg in sys.argv[3:]):
            raise Unsafe("Maven debug options are not allowed")
        check_text(" ".join(sys.argv[3:]), maven_arguments=sys.argv[3:])
        check_runtime_options()
        check_config_files(Path.cwd())
        for signum in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(signum, interrupted)
        return capture(
            sys.argv[2:],
            os.environ,
            secret_forms(os.environ),
            Path(os.environ.get("RELEASE_LOG_DIR", "/tmp/release-logs")),
        )
    except Unsafe as error:
        # Unsafe messages name only fixed policy checks, never their input values.
        print(f"Release safety check failed: {error}; values withheld.", file=sys.stderr)
        return 2
    except Exception:
        # Exception values can contain credentials. Do not echo them.
        print(
            "Release safety check or sanitized log capture failed; values withheld.",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
