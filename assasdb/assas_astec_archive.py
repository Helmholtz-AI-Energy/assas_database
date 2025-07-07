"""AssasAstecArchive class.

Module to define the AssasAstecArchive class, which contains information
about an archive uploaded to the ASSAS Astec database.
It includes properties such as upload UUID, name, date, user, description,
archive path, and result path.
"""

from uuid import uuid4


class AssasAstecArchive:
    """AssasAstecArchive class.

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
        """Initialize the AssasAstecArchive instance.

        Args:
            upload_uuid (uuid4): UUID of the upload.
            name (str): Name of the archive.
            date (str): Date of the upload.
            user (str): User who uploaded the archive.
            description (str): Description of the archive.
            archive_path (str): Path to the archive.
            result_path (str): Path to the result file.

        """
        self._upload_uuid = upload_uuid
        self._name = name
        self._date = date
        self._user = user
        self._description = description
        self._archive_path = archive_path
        self._result_path = result_path

    @property
    def upload_uuid(self) -> uuid4:
        """Return the UUID of the upload.

        Returns:
            UUID of the upload.

        """
        return self._upload_uuid

    @property
    def name(self) -> str:
        """Return the name of the archive.

        Returns:
            Name of the archive.

        """
        return self._name

    @property
    def date(self) -> str:
        """Return the date of the upload.

        Returns:
            Date of the upload.

        """
        return self._date

    @property
    def user(self) -> str:
        """Return the user who uploaded the archive.

        Returns:
            User who uploaded the archive.

        """
        return self._user

    @property
    def description(self) -> str:
        """Return the description of the archive.

        Returns:
            Description of the archive.

        """
        return self._description

    @property
    def archive_path(self) -> str:
        """Return the path to the archive.

        Returns:
            Path to the archive.

        """
        return self._archive_path

    @property
    def result_path(self) -> str:
        """Return the path to the result file.

        Returns:
            Path to the result file.

        """
        return self._result_path
