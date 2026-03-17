import unittest


class ImportSmokeTest(unittest.TestCase):
    def test_package_imports(self):
        import phraser

        self.assertTrue(hasattr(phraser, "Audio"))


if __name__ == "__main__":
    unittest.main()
