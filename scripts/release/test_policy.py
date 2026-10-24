"""CDC reactor policy and publishing preflight regression tests."""
from contextlib import redirect_stdout, redirect_stderr
from io import StringIO
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

import credentials
import guard

ROOT = Path(__file__).resolve().parents[2]


class PolicyTests(unittest.TestCase):
    def test_missing_and_placeholder_credentials_name_only_configuration(self):
        for env in ({}, {'SONATYPE_TOKEN_USERNAME': 'REPLACE_WITH_SONATYPE_TOKEN_USERNAME',
                         'SONATYPE_TOKEN_PASSWORD': 'REPLACE_WITH_SONATYPE_TOKEN_PASSWORD'},
                    {'SONATYPE_TOKEN_USERNAME': 'real-secret-user',
                     'SONATYPE_TOKEN_PASSWORD': 'REPLACE_WITH_SONATYPE_TOKEN_PASSWORD'}):
            with self.subTest(env=bool(env)), redirect_stderr(StringIO()) as output:
                self.assertEqual(credentials.check(env), 1)
                self.assertIn('SONATYPE_TOKEN_', output.getvalue())
                for value in env.values():
                    self.assertNotIn(value, output.getvalue())
        with redirect_stdout(StringIO()):
            self.assertEqual(credentials.check({'SONATYPE_TOKEN_USERNAME': 'user',
                                                'SONATYPE_TOKEN_PASSWORD': 'password'}), 0)

    def test_workflow_rejects_placeholders_before_signing(self):
        workflow = (ROOT / '.github/workflows/release.yml').read_text()
        validation, release = workflow.split('\n  release:\n', 1)
        self.assertNotIn('secrets.', validation)
        self.assertLess(release.index('credentials.py'), release.index('- name: Prepare release'))
        self.assertIn('run: python3 scripts/release/credentials.py', release)
        self.assertIn('environment: release', release)
        self.assertNotIn('cache:', release)
        result = subprocess.run(
            [sys.executable, str(ROOT / 'scripts/release/credentials.py')],
            capture_output=True, text=True, timeout=10,
            env={**os.environ, 'SONATYPE_TOKEN_USERNAME': 'REPLACE_WITH_SONATYPE_TOKEN_USERNAME',
                 'SONATYPE_TOKEN_PASSWORD': 'REPLACE_WITH_SONATYPE_TOKEN_PASSWORD'},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('missing or placeholder', result.stderr)
        self.assertNotIn('REPLACE_WITH_', result.stderr)

    def test_child_pom_cannot_override_safe_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / 'pom.xml', root)
            child = root / 'nested' / 'child'
            child.mkdir(parents=True)
            for setting in ('<gpg.bestPractices>false</gpg.bestPractices>',
                            '<arguments>-Dgpg.passphrase=synthetic-secret</arguments>',
                            '<passphrase>${env.SECRET}</passphrase>'):
                (child / 'pom.xml').write_text(
                    '<project xmlns="http://maven.apache.org/POM/4.0.0"><profiles><profile>'
                    '<configuration>' + setting + '</configuration></profile></profiles></project>')
                with self.subTest(setting=setting), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)

    def test_gpg_credentials_and_property_aliases_are_rejected_in_every_pom_scope(self):
        cases = (
            ('', '<arg>--passphrase</arg><arg>disposable-passphrase</arg>'),
            ('', '<arg>--passphrase=disposable-passphrase</arg>'),
            ('', '<arg>--passphr=disposable-passphrase</arg>'),
            ('', '<arg>--passphrase-file</arg><arg>passphrase.txt</arg>'),
            ('', '<arg>--comment</arg><arg>${env.MAVEN_GPG_PASSPHRASE}</arg>'),
            ('', '<arg>--comment</arg><arg>${session$alias.systemProperties(env.MAVEN_GPG_PASSPHRASE)}</arg>'),
            ('', '<arg>--set-filename=${env.RELEASE_GPG_PASSPHRASE}</arg>'),
            ('<name>MAVEN_GPG_PASSPHRASE</name>',
             '<arg>--comment</arg><arg>${env.${name}}</arg>'),
            ('<label>${env.MAVEN_GPG_PASSPHRASE}</label><forward>${label}</forward>',
             '<arg>--comment</arg><arg>${forward}</arg>'),
            ('<option>--passphrase</option>',
             '<arg>${option}</arg><arg>disposable-passphrase</arg>'),
        )
        with tempfile.TemporaryDirectory() as directory, patch.dict(
                os.environ, MAVEN_GPG_PASSPHRASE='disposable-passphrase'):
            root = Path(directory)
            child = root / 'nested' / 'child' / 'pom.xml'
            child.parent.mkdir(parents=True)
            for location in ('root', 'root-profile', 'child', 'child-profile'):
                for properties, arguments in cases:
                    shutil.copy(ROOT / 'pom.xml', root)
                    child.unlink(missing_ok=True)
                    properties = '<properties>' + properties + '</properties>'
                    plugin = ('<build><plugins><plugin><artifactId>maven-gpg-plugin</artifactId>'
                              '<configuration><gpgArguments>' + arguments + '</gpgArguments>'
                              '</configuration></plugin></plugins></build>')
                    body = properties + plugin
                    if location.endswith('profile'):
                        body = '<profiles><profile><id>guard-test</id>' + body + '</profile></profiles>'
                    # A minimal POM keeps the policy fixture independent of Maven execution.
                    safe = '<gpg.bestPractices>true</gpg.bestPractices>'
                    if location.endswith('profile'):
                        body = '<properties>' + safe + '</properties>' + body
                    else:
                        body = body.replace('<properties>', '<properties>' + safe, 1)
                    path = child if location.startswith('child') else root / 'pom.xml'
                    path.write_text('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                                    '<modelVersion>4.0.0</modelVersion><groupId>test</groupId>'
                                    '<artifactId>guard-test</artifactId><version>1</version>'
                                    + body + '</project>')
                    with self.subTest(location=location, arguments=arguments), \
                            self.assertRaisesRegex(guard.Unsafe, 'credential arguments'):
                        guard.check_policy(root)

    def test_safe_gpg_arguments_and_environment_name_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / 'pom.xml', root)
            child = root / 'child'
            child.mkdir()
            (child / 'pom.xml').write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>'
                '<label>release signing</label><signing.env.name>MAVEN_GPG_PASSPHRASE</signing.env.name>'
                '</properties><build><plugins><plugin><artifactId>maven-gpg-plugin</artifactId>'
                '<configuration><passphraseEnvName>${signing.env.name}</passphraseEnvName>'
                '<gpgArguments><arg>--pinentry-mode</arg><arg>loopback</arg>'
                '<arg>--comment</arg><arg>${label}</arg></gpgArguments>'
                '</configuration></plugin></plugins></build></project>')
            guard.check_policy(root)

    def test_maven_config_cannot_hide_gpg_credentials_in_property_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / 'pom.xml', root)
            config = root / '.mvn'
            config.mkdir()
            for filename, value in (
                ('maven.config', '-Drelease.alias=--passphrase=${env.MAVEN_GPG_PASSPHRASE}'),
                ('jvm.config', '-Drelease.alias="--passphr=synthetic"'),
                ('maven.config', '-Drelease.alias=${session€alias.systemProperties(env.MAVEN_GPG_PASSPHRASE)}'),
            ):
                path = config / filename
                path.write_text(value + '\n')
                with self.subTest(filename=filename), self.assertRaises(guard.Unsafe):
                    guard.check_policy(root)
                path.write_text('-Drelease.alias=--comment=public\n')
                guard.check_policy(root)
                path.unlink()

    def test_declared_custom_poms_including_nested_profile_modules_are_checked(self):
        def pom(body='', best_practices='true'):
            return ('<project xmlns="http://maven.apache.org/POM/4.0.0">'
                    '<modelVersion>4.0.0</modelVersion><groupId>test</groupId>'
                    '<artifactId>guard-test</artifactId><version>1</version><properties>'
                    '<gpg.bestPractices>' + best_practices + '</gpg.bestPractices>'
                    '</properties>' + body + '</project>')

        def modules(path, profile=False):
            body = '<modules><module>' + path + '</module></modules>'
            return ('<profiles><profile><id>guard-test</id>' + body + '</profile></profiles>'
                    if profile else body)

        for profile in (False, True):
            for nested in (False, True):
                with self.subTest(profile=profile, nested=nested), \
                        tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    (root / 'pom.xml').write_text(pom(modules(
                        'child' if nested else 'child.xml', profile)))
                    target = root / 'child.xml'
                    if nested:
                        child = root / 'child'
                        child.mkdir()
                        (child / 'pom.xml').write_text(pom(modules('nested.xml', profile)))
                        target = child / 'nested.xml'
                    target.write_text(pom(best_practices='false'))
                    with self.assertRaisesRegex(guard.Unsafe, 'best practices'):
                        guard.check_policy(root)
                    # Safe custom modules may point back to an already checked POM.
                    target.write_text(pom(modules('../pom.xml' if nested else 'pom.xml')))
                    guard.check_policy(root)

    def test_module_paths_must_be_verifiable_and_stay_within_selected_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'source'
            root.mkdir()
            original = (ROOT / 'pom.xml').read_text()
            outside = Path(directory) / 'outside.xml'
            outside.write_text('<project/>')
            (root / 'alias.xml').symlink_to(outside)
            for name, message in (('../outside.xml', 'within the selected source'),
                                  ('alias.xml', 'within the selected source'),
                                  ('${env.MODULE_PATH}', 'must be literal'),
                                  ('broken.xml', 'cannot be verified')):
                (root / 'broken.xml').write_text('<project>')
                (root / 'pom.xml').write_text(original.replace(
                    '<modules>', '<modules><module>' + name + '</module>', 1))
                with self.subTest(name=name), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)

    def test_plugin_configuration_modules_are_not_reactor_declarations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'source'
            root.mkdir()
            (Path(directory) / 'outside.xml').write_text('<project>')
            (root / 'broken.xml').write_text('<project>')
            plugin = ('<plugin><artifactId>maven-javadoc-plugin</artifactId><configuration>'
                      '<modules><module>../outside.xml</module><module>${documentation.module}</module>'
                      '<module>broken.xml</module><module/></modules>'
                      '</configuration></plugin>')
            original = (ROOT / 'pom.xml').read_text()
            for profile in (False, True):
                if profile:
                    pom = original.replace(
                        '<profiles>', '<profiles><profile><id>plugin-modules</id><build><plugins>'
                        + plugin + '</plugins></build></profile>', 1)
                else:
                    pom = original.replace('<plugins>', '<plugins>' + plugin, 1)
                for namespace in (True, False):
                    text = pom if namespace else pom.replace(
                        ' xmlns="http://maven.apache.org/POM/4.0.0"', '')
                    (root / 'pom.xml').write_text(text)
                    with self.subTest(profile=profile, namespace=namespace):
                        guard.check_policy(root)

    def test_unqualified_poms_preserve_gpg_and_publishing_policy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copy(ROOT / 'pom.xml', root)
            child = root / 'child'
            child.mkdir()
            for setting, message in (
                ('<properties><gpg.bestPractices>false</gpg.bestPractices></properties>',
                 'best practices'),
                ('<build><plugins><plugin><artifactId>maven-gpg-plugin</artifactId>'
                 '<configuration><bestPractices>false</bestPractices></configuration>'
                 '</plugin></plugins></build>', 'best practices'),
                ('<build><plugins><plugin><artifactId>central-publishing-maven-plugin</artifactId>'
                 '<version>0.10.0</version></plugin></plugins></build>', 'update Central'),
            ):
                (child / 'pom.xml').write_text('<project>' + setting + '</project>')
                with self.subTest(message=message), self.assertRaisesRegex(guard.Unsafe, message):
                    guard.check_policy(root)
            (child / 'pom.xml').write_text('<project><properties>'
                                          '<gpg.bestPractices>true</gpg.bestPractices>'
                                          '</properties></project>')
            (root / 'pom.xml').write_text((ROOT / 'pom.xml').read_text().replace(
                ' xmlns="http://maven.apache.org/POM/4.0.0"', ''))
            guard.check_policy(root)
            path = root / 'pom.xml'
            path.write_text(path.read_text().replace(
                '<profiles>', '<profiles><profile><id>bare-module</id><modules>'
                '<module>child/profile.xml</module></modules></profile>', 1))
            custom = child / 'profile.xml'
            custom.write_text('<project><profiles><profile><id>unsafe</id><build><plugins><plugin>'
                              '<artifactId>maven-gpg-plugin</artifactId><configuration>'
                              '<bestPractices>false</bestPractices></configuration>'
                              '</plugin></plugins></build></profile></profiles></project>')
            with self.assertRaisesRegex(guard.Unsafe, 'best practices'):
                guard.check_policy(root)
            custom.write_text(custom.read_text().replace('false', 'true'))
            guard.check_policy(root)

    def test_make_rejects_gpg_credential_before_signing_helper_or_maven_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ('pom.xml', 'Makefile'):
                shutil.copy(ROOT / name, root)
            child = root / 'child'
            child.mkdir()
            (child / 'pom.xml').write_text(
                '<project xmlns="http://maven.apache.org/POM/4.0.0"><properties>'
                '<label>${env.MAVEN_GPG_PASSPHRASE}</label></properties><build><plugins><plugin>'
                '<artifactId>maven-gpg-plugin</artifactId><configuration><gpgArguments>'
                '<arg>--comment</arg><arg>${label}</arg></gpgArguments>'
                '</configuration></plugin></plugins></build></project>')
            marker = root / 'child-launched'
            sentinel = root / 'sentinel.py'
            sentinel.write_text(f'#!{sys.executable}\nfrom pathlib import Path\n'
                                f'Path({str(marker)!r}).touch()\n')
            sentinel.chmod(0o700)
            gh = root / 'gh'
            gh.write_text('#!/bin/sh\nprintf "%s\\n" scylladb-publisher\n')
            gh.chmod(0o700)
            for target in ('release-prepare', 'release', 'release-dry-run'):
                result = subprocess.run(
                    ['make', target, 'RELEASE_GUARD=' + str(ROOT / 'scripts/release/guard.py'),
                     'RELEASE_SIGNING_HELPER=' + str(sentinel), 'mvn=' + str(sentinel)],
                    cwd=root, capture_output=True, text=True, timeout=10,
                    env={**os.environ, 'GITHUB_ACTIONS': 'true',
                         'PATH': str(root) + os.pathsep + os.environ['PATH'],
                         'MAVEN_GPG_PASSPHRASE': 'disposable-passphrase'})
                with self.subTest(target=target):
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn('credential arguments', result.stderr)
                    self.assertNotIn('disposable-passphrase', result.stdout + result.stderr)
                    self.assertFalse(marker.exists())

    def test_publication_contract_is_preserved(self):
        ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
        pom = ET.parse(ROOT / 'pom.xml')
        def value(path):
            return pom.findtext(path, namespaces=ns)
        self.assertEqual(value('m:properties/m:maven.compiler.target'), '11')
        self.assertEqual(value('.//m:tagNameFormat'), 'scylla-cdc-@{project.version}')
        self.assertEqual(value('.//m:autoVersionSubmodules'), 'true')
        self.assertEqual(value('.//m:excludeArtifacts'),
                         'scylla-cdc-printer,scylla-cdc-replicator,scylla-cdc-state-redis')
        self.assertIn('jar-no-fork', [e.text for e in pom.findall('.//m:goal', ns)])
        self.assertIn('jar', [e.text for e in pom.findall('.//m:goal', ns)])


if __name__ == '__main__':
    unittest.main()
