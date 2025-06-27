from uuid import uuid4


class AssasAstecArchive:
    """
    This class represents an ASTEC archive with its metadata.
    It contains information about the upload UUID, name, date, user, description,
    archive path, and result path.
    """

    def __init__(
        self,
        upload_uuid: uuid4,
        name: str,
        date: str,
        user: str,
        description: str,
        archive_path: str,
        result_path: str,
    ) -> None:
        """
        Initialize the AssasAstecArchive instance.
        :param upload_uuid: UUID of the upload.
        :param name: Name of the archive.
        :param date: Date of the upload.
        :param user: User who uploaded the archive.
        :param description: Description of the archive.
        :param archive_path: Path to the archive.
        :param result_path: Path to the result file.
        """
        self._upload_uuid = upload_uuid
        self._name = name
        self._date = date
        self._user = user
        self._description = description
        self._archive_path = archive_path
        self._result_path = result_path

    @property
    def upload_uuid(self):
        """
        Returns the UUID of the upload.
        :return: UUID of the upload.
        """
        return self._upload_uuid

    @property
    def name(self):
        """
        Returns the name of the archive.
        :return: Name of the archive.
        """
        return self._name

    @property
    def date(self):
        """
        Returns the date of the upload.
        :return: Date of the upload.
        """
        return self._date

    @property
    def user(self):
        """
        Returns the user who uploaded the archive.
        :return: User who uploaded the archive.
        """
        return self._user

    @property
    def description(self):
        """
        Returns the description of the archive.
        :return: Description of the archive.
        """
        return self._description

    @property
    def archive_path(self):
        """
        Returns the path to the archive.
        :return: Path to the archive.
        """
        return self._archive_path

    @property
    def result_path(self):
        """
        Returns the path to the result file.
        :return: Path to the result file.
        """
        return self._result_path
