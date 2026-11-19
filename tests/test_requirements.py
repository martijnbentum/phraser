import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RequirementsTest(unittest.TestCase):
    def test_requirements_match_pyproject(self):
        '''requirements.txt mirrors [project].dependencies in order.'''
        with (ROOT / 'pyproject.toml').open('rb') as stream:
            expected = tomllib.load(stream)['project']['dependencies']
        lines = (ROOT / 'requirements.txt').read_text().splitlines()
        found = [line.strip() for line in lines if line.strip()]
        self.assertEqual(found, expected)


if __name__ == '__main__':
    unittest.main()
