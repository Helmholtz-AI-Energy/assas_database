import unittest
from uuid import uuid4
from assasdb import AssasAstecArchive


class AssasAstecArchiveTest(unittest.TestCase):
    def setUp(self):
        self.upload_uuid = uuid4()
        self.name = "Test Archive"
        self.date = "2025-06-27"
        self.user = "test_user"
        self.description = "Test description"
        self.archive_path = "/path/to/archive.zip"
        self.result_path = "/path/to/result.txt"
        self.archive = AssasAstecArchive(
            self.upload_uuid,
            self.name,
            self.date,
            self.user,
            self.description,
            self.archive_path,
            self.result_path,
        )

    def test_upload_uuid(self):
        self.assertEqual(self.archive.upload_uuid, self.upload_uuid)

    def test_name(self):
        self.assertEqual(self.archive.name, self.name)

    def test_date(self):
        self.assertEqual(self.archive.date, self.date)

    def test_user(self):
        self.assertEqual(self.archive.user, self.user)

    def test_description(self):
        self.assertEqual(self.archive.description, self.description)

    def test_archive_path(self):
        self.assertEqual(self.archive.archive_path, self.archive_path)

    def test_result_path(self):
        self.assertEqual(self.archive.result_path, self.result_path)


if __name__ == "__main__":
    unittest.main()
