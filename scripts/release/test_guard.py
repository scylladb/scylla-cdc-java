"""Focused checks for release configuration and sanitized output."""

import base64
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import urllib.parse

import guard

ROOT = Path(__file__).resolve().parents[2]
CENTRAL_VERSION = re.compile(
    r"(<artifactId>central-publishing-maven-plugin</artifactId>\s*<version>)[^<]*(</version>)"
)


def with_central_version(pom, version):
    # Locate the pin by plugin, not by its current value, so dependency bumps can't
    # turn these tests into silent no-ops.
    updated, count = CENTRAL_VERSION.subn(lambda match: match[1] + version + match[2], pom)
    if count != 1:
        raise AssertionError(f"expected one central-publishing version pin, found {count}")
    return updated



class ReleaseTests(unittest.TestCase):
    def test_repository_defaults(self):
        guard.check_policy(ROOT)

    def test_unsafe_options(self):
        for value in (
            "-X",
            "-l raw.log",
            "-lraw.log",
            "-l=raw.log",
            "--log-file raw.log",
            "--log-file=raw.log",
            "-Dgpg.bestPractices=false",
            "-Dgpg.bestPractices=no",
            "-Dgpg.bestPractices=0",
            "-Dgpg.bestPractices=",
            "--define=gpg.bestPractices=maybe",
            "-define=gpg.bestPractices=false",
            "-define=javax.net.debug=all",
            "-define=http.authorization=dummy",
            "-define password=dummy",
            "-Djavax.net.debug=all",
            "-Djavax.net.debug=ssl:record:plaintext",
            "-Djavax.net.debug",
            '--define=javax.net.debug="all"',
            '-Dgpg.bestPractices="false"',
            "-Dgpg.bestPractices='false'",
            "-Dorg.slf4j.simpleLogger.logFile=raw.log",
            "-Dorg.slf4j.simpleLogger.logFile=SYSTEM.OUT",
            "-Dorg.slf4j.simpleLogger.logFile=System.out",
            "-Dorg.slf4j.simpleLogger.logFile=System.err",
            "-Dorg.slf4j.simpleLogger.logFile=System.out raw.log",
            "--debug",
            "-debug",
            '-Dorg.slf4j.simpleLogger.log.org.apache.http="debug"',
            '-Dorg.slf4j.simpleLogger."logFile"=raw.log',
            "-Dorg.slf4j.simpleLogger.log.org.apache.http='trace'",
            '-Darguments="-X"',
            "-Dgpg.passphrase=dummy",
            "-Dhttp.authorization=dummy",
            "--define=gpg.passphrase=dummy",
            "-D gpg.passphrase=dummy",
            "-Dorg.slf4j.simpleLogger.defaultLogLevel=trace",
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value)
        guard.check_text("mvn -B -ntp release:perform -DskipTests=true")
        guard.check_text('-Dgpg.bestPractices="true"')
        guard.check_text("if [[ $? -lt 127 ]]", "Makefile", maven_options=False)

    def test_complete_gpg_boolean_values(self):
        for value in (
            '-Dgpg.bestPractices="true false"',
            "-Dgpg.bestPractices='true false'",
            '"-Dgpg.bestPractices=true false"',
            '-Dgpg."bestPractices"="true false"',
            '-Dgpg.bestPractices="true "',
            '-Dgpg.bestPractices',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices=true',
        ):
            guard.check_text(value, jvm_options=True)
        for value in (
            '-Dgpg.bestPractices="true"',
            "-Dgpg.bestPractices='true'",
            '"-Dgpg.bestPractices=true"',
            '-Dgpg."bestPractices"="true"',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value, jvm_options=True)
        guard.check_text("-Dgpg.bestPractices")  # Maven CLI bare properties mean true.
        for name in ("MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS"):
            for value in ('-Dgpg.bestPractices="true false"', "-Dgpg.bestPractices"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(
                    sys, "argv", ["guard.py", "run", "mvn", "validate"]
                ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
        for name in guard.ENV_OPTIONS:
            for value in ("-Dorg.slf4j.simpleLogger.logFile=raw.log",
                          "-Dorg.slf4j.simpleLogger.logFile"):
                with self.subTest(name=name, value=value), patch.dict(
                    os.environ, {name: value}, clear=True
                ), patch.object(sys, "argv", ["guard.py", "run", "mvn", "validate"]), patch.object(
                    guard, "capture"
                ) as capture, patch("sys.stderr", io.StringIO()):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_direct_gpg_argument_boundaries(self):
        for value, expected in (("true false", 2), ("true ", 2), ("true", 0)):
            with self.subTest(value=value), patch.dict(os.environ, {}, clear=True), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "-Dgpg.bestPractices=" + value]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), expected)
                self.assertEqual(capture.called, expected == 0)

    def test_gpg_credential_aliases_at_property_and_quote_boundaries(self):
        for value in (
            '--passphrase=synthetic',
            '-Drelease.alias=--passphrase=synthetic',
            '-Drelease.alias="--passphrase=synthetic"',
            "-Drelease.alias='--passphr=synthetic'",
            '--define=release.alias=--passphrase-file=passphrase.txt',
            '-Drelease.alias="--passphrase-fd=0"',
            '-Drelease.alias=--comment=${env.MAVEN_GPG_PASSPHRASE}',
            '-Drelease.alias=${env.${name}}',
        ):
            with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value)
        for value in ('-Drelease.alias=--comment=public',
                      '-Drelease.alias="--pinentry-mode=loopback"',
                      '-Drelease.alias=MAVEN_GPG_PASSPHRASE',
                      '-Drelease.alias=${env.LANG}'):
            with self.subTest(safe=value):
                guard.check_text(value)

    def test_gpg_credential_alias_sources_reject_before_child_launch(self):
        unsafe = '-Drelease.alias=--comment=${env.MAVEN_GPG_PASSPHRASE}'
        cases = [('cli', '-Drelease.alias="--passphrase=synthetic"', False),
                 ('maven.config', '-Drelease.alias=--passphrase=${env.MAVEN_GPG_PASSPHRASE}', False),
                 ('jvm.config', '-Drelease.alias="--passphr=synthetic"', False)]
        cases += [(name, unsafe, False) for name in guard.ENV_OPTIONS]
        cases += [('cli', '-Drelease.alias=${session$alias.systemProperties(env.MAVEN_GPG_PASSPHRASE)}', False),
                  ('maven.config', '-Drelease.alias=${session€alias.systemProperties(env.MAVEN_GPG_PASSPHRASE)}', False),
                  ('maven.config', '-Drelease.alias=${project.properties(alias)}', False),
                  ('jvm.config', '-Drelease.alias=${project.class.classLoader}', False),
                  ('MAVEN_OPTS', '-Drelease.alias=${settings.servers[0].password}', False)]
        cases += [(source, '-Drelease.alias=--comment=public', True)
                  for source in ('cli', 'maven.config', 'jvm.config', 'MAVEN_OPTS')]
        cases += [('cli', '-Drelease.alias=${project.build.directory}', True)]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / '.mvn'
            config.mkdir()
            marker = root / 'maven-launched'
            maven = root / 'mvn'
            maven.write_text(f'#!{sys.executable}\nfrom pathlib import Path\n'
                             f'Path({str(marker)!r}).touch()\n')
            maven.chmod(0o700)
            for source, value, accepted in cases:
                marker.unlink(missing_ok=True)
                for name in ('maven.config', 'jvm.config'):
                    (config / name).unlink(missing_ok=True)
                env = {key: value for key, value in os.environ.items() if key not in guard.ENV_OPTIONS}
                env.update(MAVEN_GPG_PASSPHRASE='synthetic', RELEASE_LOG_DIR=str(root / 'logs'))
                arguments = [str(maven)]
                if source == 'cli':
                    arguments.append(value)
                elif source in guard.ENV_OPTIONS:
                    env[source] = value
                else:
                    (config / source).write_text(value + '\n')
                result = subprocess.run(
                    [sys.executable, str(ROOT / 'scripts/release/guard.py'), 'run', *arguments],
                    cwd=root, env=env, capture_output=True, text=True, timeout=10)
                with self.subTest(source=source, accepted=accepted):
                    self.assertEqual(result.returncode, 0 if accepted else 2, result.stderr)
                    self.assertEqual(marker.exists(), accepted)
                    self.assertNotIn('synthetic', result.stdout + result.stderr)

    def test_maven_context_expressions_allow_only_public_scalar_metadata(self):
        for value in (
            '${session.systemProperties(env.MAVEN_GPG_PASSPHRASE)}',
            '${sessionAlias.systemProperties(alias)}',
            '${session$alias.systemProperties(env.MAVEN_GPG_PASSPHRASE)}',
            '${session€alias.systemProperties(alias)}',
            '${session:-public.systemProperties(alias)}',
            '${session:0:2.systemProperties(alias)}',
            '${session}',
            '${settings.servers[0].password}',
            '${project.properties(alias)}',
            '${pom.properties[alias]}',
            '${project.getProperties()}',
            '${project.class.classLoader}',
            '${project.build.directory.class}',
            '${project.build.sourceEncoding.class}',
            '${project.reporting.outputDirectory.class}',
            '${project}',
            '${project.build}',
            '${projectLabel}',
            '${projectAlias.version}',
            '${project$alias.version}',
            '${project:-public.getProperties()}',
            '${plugin.artifacts[0].class}',
            '${mojo.plugin}',
            '${repositorySystemSession}',
            '${reactorProjects}',
            '${executedProject}',
        ):
            with self.subTest(denied=value), self.assertRaises(guard.Unsafe):
                guard.check_text(value)
        for value in ('${project.version}', '${project.build.directory}', '${pom.groupId}',
                      '${project.build.directory}/classes',
                      '${project.build.sourceEncoding}', '${pom.build.sourceEncoding}',
                      '${project.reporting.outputDirectory}/x', '--limit-modules',
                      '${project.parent.version}', '${release.alias}', '${releaseLabel}',
                      '${env.LANG}', '${signing.env.name}', 'MAVEN_GPG_PASSPHRASE',
                      '${MAVEN_OPTS:-}', '${NEXT_RELEASE[@]}', '${VALUE:0:2}', '${array[0]}',
                      '${session:-public}', '${session:0:2}', '${project:-public}',
                      '${mvn}', '$(RELEASE_GUARD)', '${{ inputs.release-version }}'):
            with self.subTest(safe=value):
                guard.check_text(value)

    def test_direct_gpg_aliases_and_nested_arguments(self):
        for option in ("-D", "-define=", "--define="):
            with self.subTest(option=option), self.assertRaises(guard.Unsafe):
                argument = option + "gpg.bestPractices=true false"
                guard.check_text(argument, maven_arguments=[argument])
        with self.assertRaises(guard.Unsafe):
            guard.check_text("-D gpg.bestPractices=true false",
                             maven_arguments=["-D", "gpg.bestPractices=true false"])
        nested = "-Darguments=-Dgpg.bestPractices=true -DskipTests=true"
        guard.check_text(nested, maven_arguments=[nested])

    def test_direct_debug_options(self):
        guard.check_text("-Xmx1g -Xms256m")
        for option in ("-Xq", "-debug"):
            with self.subTest(option=option), patch.object(
                sys, "argv", ["guard.py", "run", "mvn", option]
            ), patch.object(guard, "capture") as capture, patch("sys.stderr", io.StringIO()):
                self.assertEqual(guard.main(), 2)
                capture.assert_not_called()

    def test_maven_argument_sources_reject_debug_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / ".mvn").mkdir()
            with patch.object(
                sys, "argv", ["guard.py", "run", "mvn", "validate"]
            ), patch.object(guard, "capture", return_value=0) as capture, patch("sys.stderr", io.StringIO()):
                with patch.dict(os.environ, {"MAVEN_ARGS": "-Xvalidate"}, clear=True):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                with patch.dict(os.environ, {}, clear=True), patch.object(
                    guard.Path, "cwd", return_value=root
                ):
                    (root / ".mvn/maven.config").write_text("-Xvalidate")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-Dgpg.bestPractices=true false")
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                    (root / ".mvn/maven.config").write_text("-DargLine=-Xmx512m -XX:+UseG1GC")
                    self.assertEqual(guard.main(), 0)
                    (root / ".mvn/maven.config").unlink()
                    (root / ".mvn/jvm.config").write_text("-Dgpg.bestPractices")
                    self.assertEqual(guard.main(), 2)
                    (root / ".mvn/jvm.config").write_text("-Xmx1g")
                    self.assertEqual(guard.main(), 0)
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_ARGS": "-DargLine='-Xmx512m -XX:+UseG1GC'"}, clear=True):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                capture.reset_mock()
                with patch.dict(os.environ, {"MAVEN_OPTS": "-Xmx1g"}, clear=True):
                    self.assertEqual(guard.main(), 0)
                    capture.assert_called_once()

    def test_maven_aliases_are_rejected_across_argument_sources(self):
        for value in ("--X", "--l raw.log", "--l=raw.log", "--lo=raw.log",
                      "--log raw.log", "--log-f=raw.log", "--log-fi raw.log",
                      "--Dgpg.bestPractices=false", "--D=gpg.bestPractices=false",
                      "--Dorg.slf4j.simpleLogger.logFile=raw.log"):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shutil.copy(ROOT / "pom.xml", root / "pom.xml")
                config = root / ".mvn/maven.config"
                config.parent.mkdir()
                config.write_text(value.replace(" ", "\n"))
                with self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
                with patch.object(guard, "capture") as capture, \
                     patch.dict(os.environ, {"MAVEN_ARGS": value}, clear=True), \
                     patch.object(sys, "argv", ["guard.py", "run", "mvn"]):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()
                with patch.object(guard, "capture") as capture, \
                     patch.dict(os.environ, {}, clear=True), \
                     patch.object(sys, "argv", ["guard.py", "run", "mvn", *value.split()]):
                    self.assertEqual(guard.main(), 2)
                    capture.assert_not_called()

    def test_required_signing_and_publishing_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pom = (ROOT / "pom.xml").read_text()
            for case, text, message in (
                ("bestPractices", pom.replace("<gpg.bestPractices>true</gpg.bestPractices>", ""),
                 "enable GPG best practices"),
                ("central 0.8.0", with_central_version(pom, "0.8.0"), "update Central publishing"),
                ("central 0.10.0", with_central_version(pom, "0.10.0"),
                 "update Central publishing"),
            ):
                (root / "pom.xml").write_text(text)
                with self.subTest(case=case), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)

    def test_disabled_gpg_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("false", "no", "0", "", "true"):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<artifactId>maven-gpg-plugin</artifactId>",
                    "<artifactId>maven-gpg-plugin</artifactId>"
                    f"<configuration><bestPractices>{value}</bestPractices></configuration>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value):
                    if value == "true":
                        guard.check_policy(root)
                    else:
                        with self.assertRaises(guard.Unsafe):
                            guard.check_policy(root)

    def test_disabled_gpg_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for value in ("false", "no", "0", ""):
                pom = (ROOT / "pom.xml").read_text().replace(
                    "<profiles>",
                    "<profiles><profile><id>unsafe</id><properties>"
                    f"<gpg.bestPractices>{value}</gpg.bestPractices></properties></profile>",
                )
                (root / "pom.xml").write_text(pom)
                with self.subTest(value=value), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_newer_central_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pom = (ROOT / "pom.xml").read_text()
            for version in ("0.11.0", "0.11.1", "0.12.0", "1.0.0"):
                (root / "pom.xml").write_text(with_central_version(pom, version))
                with self.subTest(version=version):
                    guard.check_policy(root)

    def test_option_locations_hide_values(self):
        for text in ("safe\ncurl -X POST secret", "safe\n-X", "safe\n--define\npassword=secret"):
            with patch("sys.stderr", new_callable=io.StringIO) as output:
                with self.assertRaises(guard.Unsafe):
                    guard.check_text(text, "Makefile")
                self.assertIn("Makefile:2", output.getvalue())
                self.assertNotIn("secret", output.getvalue())
        for text in ("-Dmaven.repo.local=/x/repo-f", "-Dfoo=a-s", "-fae -ff -llr"):
            guard.check_text(text)

    def test_termination_signals_stop_maven(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ready = root / "ready"
                mvn = root / "mvn"
                mvn.write_text(f"#!{sys.executable}\nimport os, time\n"
                               "os.write(1, b'-Dpassword=\"first\\nsecret tail')\n"
                               f"open({str(ready) + '.tmp'!r}, 'w').write(str(os.getpid()))\n"
                               f"os.rename({str(ready) + '.tmp'!r}, {str(ready)!r})\n"
                               "time.sleep(30)\n")
                mvn.chmod(0o755)
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / "scripts/release/guard.py"), "run", "mvn"],
                    env=dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                             RELEASE_LOG_DIR=str(root / "logs")),
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    deadline = time.monotonic() + 5
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child = int(ready.read_text())
                    process.send_signal(signum)
                    stdout, stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode, 128 + signum)
                    for name, console in (("stdout", stdout), ("stderr", stderr)):
                        self.assertEqual(console, guard.CANCELLED_LOG)
                        self.assertEqual((root / "logs" / (name + ".log")).read_bytes(), console)
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child, 0)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()

    def test_startup_cancellation_stops_child_group_before_constructor_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ready, observation = root / "ready", root / "observation"
            child = (
                "import json,os,signal,subprocess,sys,time\nfrom pathlib import Path\n"
                "descendant=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'])\n"
                "def stop(signum,frame):\n"
                " descendant.terminate()\n descendant.wait(timeout=2)\n raise SystemExit(128+signum)\n"
                "signal.signal(signal.SIGTERM,stop)\n"
                f"Path({str(ready)!r}).write_text(json.dumps({{'pid':os.getpid(),'child':descendant.pid,"
                "'blocked':sorted(map(int,signal.pthread_sigmask(signal.SIG_BLOCK,[]))) }))\n"
                "time.sleep(30)\n")
            script = (
                "import json,os,signal,subprocess,sys,time\nfrom pathlib import Path\nimport guard\n"
                "owned=[]\noriginal=subprocess.Popen._execute_child\n"
                "def acquiring(self,*args,**kwargs):\n"
                " original(self,*args,**kwargs)\n owned.append(self)\n"
                " deadline=time.monotonic()+5\n"
                f" while not Path({str(ready)!r}).exists() and time.monotonic()<deadline:time.sleep(0.01)\n"
                " os.kill(os.getpid(),signal.SIGTERM)\n"
                "subprocess.Popen._execute_child=acquiring\n"
                "signal.signal(signal.SIGTERM,guard.interrupted)\n"
                "try:\n"
                f" guard.capture([sys.executable,'-c',{child!r}],os.environ,[],Path({str(root / 'logs')!r}))\n"
                "finally:\n"
                " if owned:\n"
                "  process=owned[0]\n  process.poll()\n"
                "  try:\n"
                f"   state=json.loads(Path({str(ready)!r}).read_text())\n"
                "   alive=[]\n"
                "   for pid in (state['pid'],state['child']):\n"
                "    try:os.kill(pid,0);alive.append(pid)\n"
                "    except ProcessLookupError:pass\n"
                f"   Path({str(observation)!r}).write_text(json.dumps({{'alive':alive,'blocked':state['blocked']}}))\n"
                "  finally:\n"
                # Reap the controlled family even when this regression fails.
                "   try:os.killpg(process.pid,signal.SIGTERM)\n"
                "   except ProcessLookupError:pass\n"
                "   process.wait(timeout=3)\n")
            process = subprocess.Popen([sys.executable, "-c", script],
                                       cwd=ROOT / "scripts/release", stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            try:
                stdout, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143, stderr)
                state = json.loads(observation.read_text())
                self.assertEqual(state["alive"], [])
                self.assertEqual(state["blocked"], sorted(map(
                    int, signal.pthread_sigmask(signal.SIG_BLOCK, []))))
                self.assertEqual(stdout, guard.CANCELLED_LOG)
                self.assertEqual(stderr, guard.CANCELLED_LOG)
            finally:
                if ready.exists():
                    try:
                        os.killpg(json.loads(ready.read_text())["pid"], signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=3)

    def test_encoded_and_multiline_credentials(self):
        env = {
            "MAVEN_GPG_PASSPHRASE": "first line\nsecond line+/=",
            "SONATYPE_TOKEN_USERNAME": "dummy-user",
            "SONATYPE_TOKEN_PASSWORD": "dummy-password",
        }
        for value in [*env.values(), "dummy-user:dummy-password"]:
            for encoded in (
                value,
                base64.b64encode(value.encode()).decode(),
                urllib.parse.quote(value, safe=""),
                urllib.parse.quote(value),
                urllib.parse.quote(urllib.parse.quote(value)),
                urllib.parse.quote(urllib.parse.quote(value), safe=""),
                urllib.parse.quote_plus(value),
            ):
                with self.subTest(encoded=encoded):
                    self.assertEqual(
                        b"[REDACTED]", guard.redact(encoded.encode(), guard.secret_forms(env))
                    )
        self.assertEqual(
            guard.redact(b'Authorization: Bearer unknown\n-Dpassword="a\nb"', []),
            b"Authorization: [REDACTED]\n-Dpassword=[REDACTED]",
        )

    def test_xml_escaped_credentials_in_both_streams(self):
        secret = "xml&<tag>\"'secret"
        forms = (
            "xml&amp;&lt;tag&gt;\"'secret",
            "xml&amp;&lt;tag&gt;&quot;&apos;secret",
            "xml&amp;&lt;tag&gt;&quot;&#x27;secret",
            "xml&amp;&lt;tag&gt;&#34;&#39;secret",
            "xml&amp;&lt;tag&gt;&#x22;&#x27;secret",
        )
        output = ("\n".join(forms) + "\n").encode()
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c",
                       f"import os; os.write(1, {output!r}); os.write(2, {output!r})"]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script], capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE=secret),
                timeout=10,
            )
            self.assertEqual(result.returncode, 0)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, b"[REDACTED]\n" * len(forms))
                self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_quoted_authorization_headers(self):
        for header in ("Authorization", "Proxy-Authorization"):
            for quote in ("", "'", '"'):
                with self.subTest(header=header, quote=quote):
                    data = f"{quote}{header}{quote}: {quote}Bearer unknown{quote}\n".encode()
                    self.assertNotIn(b"unknown", guard.redact(data, []))

    def test_quoted_credential_arguments(self):
        for quote in (b"'", b'"'):
            for option in (b"-Dpassword=", b"--define=token=", b"--define secret=",
                           b"-define=token=", b"-define secret="):
                with self.subTest(quote=quote, option=option):
                    result = guard.redact(quote + option + b"two words\nlast" + quote, [])
                    self.assertNotIn(b"words", result)
                    self.assertNotIn(b"last", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_escaped_quotes_in_credential_arguments(self):
        for quote in (b"'", b'"'):
            secret = b"alpha" + b"\\" + quote + b"beta gamma"
            for value in (
                b"-Dpassword=" + quote + secret + quote,
                quote + b"-Dpassword=" + secret + quote,
            ):
                with self.subTest(value=value):
                    result = guard.redact(value, [])
                    self.assertNotIn(b"beta", result)
                    self.assertNotIn(b"gamma", result)
                    self.assertIn(b"[REDACTED]", result)

    def test_secrets_matching_credential_markers(self):
        for marker, output in (
            ("password", b"-Ddb.password=unregistered-value"),
            ("Authorization", b"Authorization: Bearer unregistered-value"),
        ):
            with self.subTest(marker=marker):
                result = guard.redact(output, guard.secret_forms({"TOKEN": marker}))
                self.assertNotIn(b"unregistered-value", result)
                self.assertNotIn(marker.encode(), result)

    def test_overlapping_structured_and_literal_secrets(self):
        for secret in ("prefix-Authorization: suffix", "prefix-Dpassword=value tail"):
            with self.subTest(secret=secret):
                result = guard.redact(secret.encode(), guard.secret_forms({"TOKEN": secret}))
                self.assertEqual(result, b"[REDACTED]")
        self.assertEqual(guard.redact(b"ababa", [b"aba"]), b"[REDACTED]")

    def test_split_output_artifacts_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            # Use a real subprocess to verify both console streams and the resulting files.
            command = [
                sys.executable,
                "-c",
                "import os\nfor fd in (1, 2):\n"
                ' for part in (b"diagnostic: ", b"dummy-", b"secret\\n"):\n'
                "  os.write(fd, part)\nraise SystemExit(17)",
            ]
            script = (
                "import guard, os\nfrom pathlib import Path\n"
                f"raise SystemExit(guard.capture({command!r}, os.environ, "
                f"guard.secret_forms(os.environ), Path({directory!r})))"
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                cwd=ROOT / "scripts/release",
                env=dict(os.environ, MAVEN_GPG_PASSPHRASE="dummy-secret"),
                timeout=10,
            )
            self.assertEqual(result.returncode, 17)
            for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                self.assertEqual(console, (Path(directory) / (name + ".log")).read_bytes())
                self.assertEqual(console, b"diagnostic: [REDACTED]\n")

    def test_capture_read_failure_preserves_completed_maven_status(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                process = unittest.mock.Mock()
                process.poll.return_value = status
                process.wait.return_value = status
                with patch.object(guard.subprocess, "Popen", return_value=process), \
                     patch.object(guard.selectors, "DefaultSelector", side_effect=OSError("secret")), \
                     patch.object(guard.os, "killpg"), patch.object(guard, "write_logs") as logs:
                    self.assertEqual(guard.capture(["mvn"], {}, [], Path(directory)), status)
                    logs.assert_called_once_with([guard.FAILED_LOG, guard.FAILED_LOG], Path(directory))

    def test_configured_maven_executable(self):
        for executable in ("/usr/bin/mvn", "./mvnw", "custom-maven"):
            with self.subTest(executable=executable), \
                 patch.object(sys, "argv", ["guard.py", "run", executable, "release:prepare"]), \
                 patch.object(guard, "capture", return_value=0) as capture, \
                 patch.object(guard, "check_runtime_options"):
                self.assertEqual(guard.main(), 0)
                self.assertEqual(capture.call_args.args[0], [executable, "release:prepare"])

    def test_output_failures_preserve_maven_status(self):
        for status in (0, 17):
            for failure in ("directory", "artifact", "stdout", "stderr"):
                with self.subTest(status=status, failure=failure), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    if failure == "directory":
                        logs.write_text("blocked")
                    elif failure == "artifact":
                        logs.mkdir()
                        (logs / "stdout.log").mkdir()
                    command = [sys.executable, "-c",
                               f"import os; os.write(1, b'out\\n'); os.write(2, b'err\\n'); raise SystemExit({status})"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    read_fd, write_fd = os.pipe()
                    os.close(read_fd)
                    try:
                        result = subprocess.run(
                            [sys.executable, "-c", script], cwd=ROOT / "scripts/release",
                            stdout=write_fd if failure == "stdout" else subprocess.PIPE,
                            stderr=write_fd if failure == "stderr" else subprocess.PIPE,
                            timeout=10,
                        )
                    finally:
                        os.close(write_fd)
                    self.assertEqual(result.returncode, status)
                    if failure != "stdout":
                        self.assertEqual(result.stdout, b"out\n")
                    if failure != "stderr":
                        self.assertIn(b"err\n", result.stderr)
                        self.assertIn(b"values withheld", result.stderr)
                    if failure in ("stdout", "stderr"):
                        self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if failure != "directory":
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_sanitization_failure_writes_fixed_logs(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    "guard.redact = lambda *_: (_ for _ in ()).throw(ValueError('secret'))\n"
                    f"raise SystemExit(guard.capture([{sys.executable!r}, '-c', "
                    f"'print(\"secret\"); raise SystemExit({status})'], os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=10)
                self.assertEqual(result.returncode, status)
                self.assertEqual(result.stdout, guard.FAILED_LOG)
                self.assertEqual(result.stderr, guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stdout.log").read_bytes(), guard.FAILED_LOG)
                self.assertEqual((Path(directory) / "stderr.log").read_bytes(), guard.FAILED_LOG)

    def test_overflow_drains_without_interrupting_maven(self):
        for status in (0, 17):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                marker = Path(directory) / "completed"
                command = [sys.executable, "-c",
                           "import os\nfor fd in (1, 2):\n"
                           " os.write(fd, b'-Dpassword=\"first\\nsecret tail' * 100)\n"
                           f"open({str(marker)!r}, 'w').write('done')\nraise SystemExit({status})"]
                script = (
                    "import guard, os\nfrom pathlib import Path\nguard.LIMIT = 64\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({directory!r})))"
                )
                result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                        cwd=ROOT / "scripts/release", timeout=5)
                self.assertEqual(result.returncode, status)
                self.assertEqual(marker.read_text(), "done")
                for name, console in (("stdout", result.stdout), ("stderr", result.stderr)):
                    self.assertEqual(console, guard.OVERFLOW_LOG)
                    self.assertEqual((Path(directory) / (name + ".log")).read_bytes(), console)

    def test_interruption_during_log_writes_does_not_replay_output(self):
        for signum in (signal.SIGTERM, signal.SIGHUP):
            for partial in (False, True):
                with self.subTest(signum=signum, partial=partial), tempfile.TemporaryDirectory() as directory:
                    logs = Path(directory) / "logs"
                    calls = Path(directory) / "writes"
                    command = [sys.executable, "-c",
                               "import os; os.write(1, b'out\\n'); os.write(2, b'err\\n')"]
                    script = (
                        "import guard, os\nfrom pathlib import Path\n"
                        "original = guard.write_logs\n"
                        "def interrupted_write(clean, logs):\n"
                        f" with Path({str(calls)!r}).open('a') as out: out.write('called\\n')\n"
                        + (" logs.mkdir(parents=True, exist_ok=True)\n"
                           " with (logs / 'stdout.log').open('ab') as out: out.write(clean[0])\n"
                           if partial else " original(clean, logs)\n")
                        + f" guard.interrupted({int(signum)}, None)\n"
                        "guard.write_logs = interrupted_write\n"
                        f"raise SystemExit(guard.capture({command!r}, os.environ, [], Path({str(logs)!r})))"
                    )
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertEqual(result.returncode, 128 + signum)
                    self.assertEqual(calls.read_text(), "called\n")
                    self.assertEqual((logs / "stdout.log").read_bytes(), b"out\n")
                    if partial:
                        self.assertFalse((logs / "stderr.log").exists())
                    else:
                        self.assertEqual(result.stdout, b"out\n")
                        self.assertEqual(result.stderr, b"err\n")
                        self.assertEqual((logs / "stderr.log").read_bytes(), b"err\n")

    def test_success_waits_for_pipe_connected_descendant_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'completed'
            command = [sys.executable, '-c',
                       'import os,time\nchild=os.fork()\nif child == 0:\n'
                       ' time.sleep(0.5)\n os.write(1,b"descendant complete\\n")\n'
                       f' open({str(marker)!r},"w").write("done")\n os._exit(0)\n'
                       'os._exit(0)']
            script = (
                'import guard,os\nfrom pathlib import Path\nguard.DRAIN_GRACE=0.01\n'
                f'raise SystemExit(guard.capture({command!r},os.environ,[],Path({directory!r})))'
            )
            result = subprocess.run([sys.executable, '-c', script], capture_output=True,
                                    cwd=ROOT / 'scripts/release', timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(marker.read_text(), 'done')
            self.assertEqual(result.stdout, b'descendant complete\n')
            self.assertEqual((Path(directory) / 'stdout.log').read_bytes(), result.stdout)

    def test_inherited_pipes_have_bounded_safe_capture(self):
        for complete_stderr in (False, True):
            with self.subTest(complete_stderr=complete_stderr), tempfile.TemporaryDirectory() as directory:
                pid_file = Path(directory) / "child"
                command = [sys.executable, "-c",
                           "import os, time\nchild = os.fork()\nif child == 0:\n"
                           + (" os.close(2)\n" if complete_stderr else "")
                           + " os.write(1, b'known-secret-prefix')\n"
                           " time.sleep(30)\n os._exit(0)\n"
                           f"open({str(pid_file)!r}, 'w').write(str(child))\n"
                           + ("os.write(2, b'-Dpassword=\"first\\nunknown secret tail\"')\n"
                              if complete_stderr else "os.write(2, b'-Dpassword=\"first\\nunknown secret tail')\n")
                           + "raise SystemExit(17)"]
                script = (
                    "import guard, os\nfrom pathlib import Path\n"
                    f"raise SystemExit(guard.capture({command!r}, os.environ, [b'known-secret-prefix-tail'], Path({directory!r})))"
                )
                started = time.monotonic()
                try:
                    result = subprocess.run([sys.executable, "-c", script], capture_output=True,
                                            cwd=ROOT / "scripts/release", timeout=5)
                    self.assertLess(time.monotonic() - started, 4)
                    self.assertEqual(result.returncode, 17)
                    self.assertEqual(result.stdout, guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stdout.log").read_bytes(), result.stdout)
                    if complete_stderr:
                        self.assertEqual(result.stderr, b'-Dpassword=[REDACTED]')
                    else:
                        self.assertEqual(result.stderr, guard.INCOMPLETE_LOG)
                    self.assertEqual((Path(directory) / "stderr.log").read_bytes(), result.stderr)
                finally:
                    if pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass


if __name__ == "__main__":
    unittest.main()
