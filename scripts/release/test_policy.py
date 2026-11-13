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
        self.assertIn('run: python3 "$RUNNER_TEMP/release-tooling/credentials.py"', release)
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



if __name__ == "__main__":
    unittest.main()
