#!/usr/bin/env python3
"""Fail closed before signing or contacting Central; never print secret values."""
import os
import sys


def check(env):
    invalid = [name for name in ('SONATYPE_TOKEN_USERNAME', 'SONATYPE_TOKEN_PASSWORD')
               if not env.get(name, '').strip()
               or env[name].strip().startswith('REPLACE_WITH_')]
    if invalid:
        print('Configure release environment secrets: ' + ', '.join(invalid)
              + ' (missing or placeholder).', file=sys.stderr)
        return 1
    print('Publishing credential preflight passed.')
    return 0


if __name__ == '__main__':
    sys.exit(check(os.environ))
