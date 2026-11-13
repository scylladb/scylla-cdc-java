#!/usr/bin/env python3
"""Check the supported release configuration without interpreting Maven arguments."""

from pathlib import Path
import re
import sys
import xml.etree.ElementTree as ET

# Release commands construct their options instead of parsing inherited inputs.
ENV_OPTIONS = (
    "MAVEN_ARGS", "MAVEN_OPTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS",
    "_JAVA_OPTIONS", "MAVEN_DEBUG_OPTS", "MAVEN_CONFIG", "MAVEN_BASEDIR", "MAVEN_PROJECTBASEDIR",
)


class Unsafe(ValueError):
    pass


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
    check_config_files(root)


def check_config_files(root):
    # The Maven launcher uses the nearest .mvn directory, including ancestors.
    root = next((parent for parent in (root, *root.parents)
                 if (parent / ".mvn").is_dir()), root)
    for name in ("maven.config", "jvm.config"):
        path = root / ".mvn" / name
        if path.exists() and path.read_text().strip():
            raise Unsafe("custom Maven/JVM option files are unsupported for releases")


def maven_environment(env):
    clean = {name: value for name, value in env.items()
             if name not in (*ENV_OPTIONS, "MAKEFLAGS", "MAKEOVERRIDES", "GH_TOKEN", "GITHUB_TOKEN")}
    clean["MAVEN_SKIP_RC"] = "true"
    return clean


def main():
    try:
        if sys.argv[1:] != ["check"]:
            raise Unsafe("expected check")
        check_policy(Path.cwd())
        print("Release configuration checks passed.")
        return 0
    except Unsafe as error:
        print(f"Release configuration check failed: {error}.", file=sys.stderr)
    except Exception:
        print("Release configuration could not be checked; values withheld.", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
