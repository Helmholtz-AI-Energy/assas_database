from typing import List
from uuid import uuid4
from datetime import datetime


class AssasDocumentFileStatus:
    """
    AssasDocumentFileStatus

    Represents the status of a document file in the ASSAS database.
    This class provides constants for various file statuses.
    """

    UPLOADED = "Uploaded"
    CONVERTING = "Converting"
    VALID = "Valid"
    INVALID = "Invalid"


class AssasDocumentFile:
    """
    AssasDocumentFile

    Represents a document file in the ASSAS database.
    This class provides methods to manage the document's metadata, including
    general metadata, system values, and key-value pairs within the document.
    """

    def __init__(self, document: dict = None) -> None:
        """
        Initializes the AssasDocumentFile instance.

        Args:
            document (dict, optional):
            Initial document data. Defaults to an empty dictionary.

        Returns:
            None
        """
        self.document = document or {}

    def get_document(self) -> dict:
        """
        Returns a copy of the document.

        Args:
            None

        Returns:
            dict: A copy of the document.

        Example:
            document_copy = AssasDocumentFile.get_document()
        """
        return self.document.copy()

    def set_document(self, document: dict) -> None:
        """
        Sets the document to the provided dictionary.

        Args:
            document (dict): The new document data.

        Returns:
            None

        Raises:
            TypeError: If `document` is not a dictionary.

        Example:
            AssasDocumentFile.set_document({"key": "value"})
        """
        if not isinstance(document, dict):
            raise TypeError("Document must be a dictionary.")

        self.document = document

    def extend_document(self, add_document: dict) -> None:
        """
        Extends the current document with additional key-value pairs.

        Args:
            add_document (dict): The additional data to merge into the document.

        Returns:
            None

        Raises:
            TypeError: If `add_document` is not a dictionary.
        """
        if not isinstance(add_document, dict):
            raise TypeError("Additional document must be a dictionary.")

        self.document.update(add_document)

    def set_general_meta_values(
        self,
        meta_name: str,
        meta_description: str,
    ) -> None:
        """
        Sets general metadata values for the document.

        Args:
            meta_name (str): The name of the document.
            meta_description (str): A description of the document.

        Returns:
            None

        Example:
            AssasDocumentFile.set_general_meta_values(
                meta_name="Sample Document",
                meta_description="This is a sample document description."
            )
        """

        self.document["meta_name"] = meta_name
        self.document["meta_description"] = meta_description

    def set_meta_data_values(
        self,
        meta_data_variables: List[dict],
    ) -> None:
        """
        Sets metadata variables for the document.

        Args:
            meta_data_variables (List[dict]):
            A list of dictionaries containing metadata variables.

        Returns:
            None

        Example:
            AssasDocumentFile.set_meta_data_values([
                {"key": "author", "value": "John Doe"},
                {"key": "version", "value": "1.0"},
            ])
        """
        self.document["meta_data_variables"] = meta_data_variables

    def set_value(self, key: str, value: str) -> None:
        """
        Sets a key-value pair in the document.

        Args:
            key (str): The key to set.
            value (str): The value to set for the key.

        Returns:
            None

        Example:
            AssasDocumentFile.set_value("author", "John Doe")
        """
        self.document[key] = value

    def get_value(self, key: str) -> str:
        """
        Retrieves the value associated with a key in the document.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            str: The value associated with the key.

        Raises:
            KeyError: If the key does not exist in the document.

        Example:
            value = AssasDocumentFile.get_value("author")
        """
        return self.document[key]

    def delete_key(self, key: str) -> bool:
        """
        Deletes a key from the document.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was found and deleted, False otherwise.

        Example:
            was_deleted = AssasDocumentFile.delete_key("author")
        """
        is_in = False
        if key in self.document:
            is_in = True
            del self.document[key]

        return is_in

    def set_system_values(
        self,
        system_uuid: str,
        system_upload_uuid: str,
        system_date: str,
        system_path: str,
        system_result: str,
        system_size: str,
        system_user: str,
        system_download: str,
        system_status: str,
    ) -> None:
        """
        Sets system-related values for the document.

        Args:
            system_uuid (str): The UUID of the system.
            system_upload_uuid (str): The upload UUID of the system.
            system_date (str): The date associated with the system.
            system_path (str): The path of the system file.
            system_result (str): The result of the system operation.
            system_size (str): The size of the system file.
            system_user (str): The user associated with the system.
            system_download (str): The download link for the system file.
            system_status (str): The status of the system file.

        Returns:
            None

        Example:
            AssasDocumentFile.set_system_values(
                system_uuid="123e4567-e89b-12d3-a456-426614174000",
                system_upload_uuid="123e4567-e89b-12d3-a456-426614174001",
                system_date=datetime.now().isoformat(),
                system_path="/path/to/system/file",
                system_result="success",
                system_size="2048",
                system_user="admin",
                system_download="http://example.com/download",
                system_status=AssasDocumentFileStatus.UPLOADED
            )
        """

        self.document["system_uuid"] = system_uuid
        self.document["system_upload_uuid"] = system_upload_uuid
        self.document["system_date"] = system_date
        self.document["system_path"] = system_path
        self.document["system_result"] = system_result
        self.document["system_size"] = system_size
        self.document["system_user"] = system_user
        self.document["system_download"] = system_download
        self.document["system_status"] = system_status

    def set_system_values_dict(self, system_values: dict) -> None:
        """
        Sets system-related values for the document.

        Args:
            system_values (dict): A dictionary containing system-related values.
                Required keys:
                    - "system_uuid"
                    - "system_upload_uuid"
                    - "system_date"
                    - "system_path"
                    - "system_result"
                    - "system_size"
                    - "system_user"
                    - "system_download"
                    - "system_status"

        Raises:
            TypeError: If `system_values` is not a dictionary.
            KeyError: If required keys are missing in `system_values`.

        Returns:
            None

        Example:
            AssasDocumentFile.set_system_values_dict({
                "system_uuid": "123e4567-e89b-12d3-a456-426614174000",
                "system_upload_uuid": "123e4567-e89b-12d3-a456-426614174001",
                "system_date": datetime.now().isoformat(),
                "system_path": "/path/to/system/file",
                "system_result": "success",
                "system_size": "2048",
                "system_user": "admin",
                "system_download": "http://example.com/download",
                "system_status": AssasDocumentFileStatus.UPLOADED
            })
        """
        if not isinstance(system_values, dict):
            raise TypeError("system_values must be a dictionary.")

        required_keys = [
            "system_uuid",
            "system_upload_uuid",
            "system_date",
            "system_path",
            "system_result",
            "system_size",
            "system_user",
            "system_download",
            "system_status",
        ]

        missing_keys = [key for key in required_keys if key not in system_values]
        if missing_keys:
            raise KeyError(f"Missing required keys in system_values: {missing_keys}")

        # Update the document with system values
        self.document.update(system_values)

    @staticmethod
    def get_test_document_file(system_upload_uuid: str = None) -> dict:
        """
        Generates a test document with predefined values for testing purposes.

        Args:
            system_upload_uuid (str, optional): The UUID for the system upload.
                If not provided, a random UUID will be generated.

        Returns:
            dict: A dictionary representing the test document.

        Example:
            test_document = AssasDocumentFile.get_test_document_file()
        """
        return {
            "system_uuid": str(uuid4()),
            "system_upload_uuid": system_upload_uuid or str(uuid4()),
            "system_date": datetime.now().isoformat(),
            "system_path": "/path/to/test/file",
            "system_result": "success",
            "system_size": "1024",
            "system_user": "test_user",
            "system_download": "http://example.com/download",
            "system_status": "uploaded",
        }
