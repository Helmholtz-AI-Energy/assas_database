"""RADAR4KIT OAuth2 Client and API Handler for ASSAS Database Integration."""

import os
import requests
import logging
import tempfile
import argparse

from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class RadarOAuthClient:
    """OAuth2 Client for RADAR4KIT API access."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        username: str,
        password: str,
        workspace_id: str,
        oauth_url: str = "https://test.radar-service.eu/radar/api/tokens",
    ) -> None:
        """Initialize OAuth client.

        Args:
            client_id: OAuth Client ID (assas-client)
            client_secret: OAuth Client Secret
            redirect_url: Redirect URL after authentication
            username: Optional username for OAuth2
            password: Optional password for OAuth2
            oauth_url: Token endpoint URL
            workspace_id: Workspace ID for RADAR

        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        self.username = username
        self.password = password
        self.workspace_id = workspace_id
        self.oauth_url = oauth_url
        self.access_token = None
        self.session = requests.Session()

        logger.info("Initialized RadarOAuthClient")
        logger.info(f"Client ID: {self.client_id}")
        logger.info(f"Username: {self.username}")
        logger.info(f"Redirect URL: {self.redirect_url}")
        logger.info(f"OAuth URL: {self.oauth_url}")
        logger.info(f"Workspace ID: {self.workspace_id}")

    def get_access_token(self) -> Optional[str]:
        """Get access token using OAuth2 Client Credentials flow.

        Returns:
            Access token if successful, None otherwise

        """
        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "redirectUrl": self.redirect_url,
            "userName": self.username,
            "userPassword": self.password,
        }

        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        logger.debug(f"Requesting OAuth access token with payload: {payload}")

        try:
            response = requests.post(
                self.oauth_url,
                json=payload,
                headers=headers,
            )

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                logger.info("Successfully obtained access token")
                return self.access_token
            else:
                logger.error(
                    f"Failed to get token: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None

    @staticmethod
    def plot_workspace_properties(workspace_info: Dict[str, Any]) -> None:
        """Display all workspace properties in a formatted way.

        Args:
            workspace_info: Workspace data dictionary from RADAR API

        """
        logger.info("\n" + "=" * 80)
        logger.info("RADAR WORKSPACE INFORMATION")
        logger.info("=" * 80)

        # Basic Information
        logger.info("\nBASIC INFORMATION")
        logger.info("-" * 80)
        logger.info(f"ID:                {workspace_info.get('id')}")
        logger.info(f"Parent ID:         {workspace_info.get('parentId')}")
        logger.info(f"Created:           {workspace_info.get('createdDate')}")
        logger.info(f"Last Modified:     {workspace_info.get('lastModifiedDate')}")

        # Descriptive Metadata
        desc_meta: Dict[str, Any] = workspace_info.get("descriptiveMetadata", {})
        logger.info("\nDESCRIPTIVE METADATA")
        logger.info("-" * 80)
        logger.info(f"Title:             {desc_meta.get('title')}")
        logger.info(f"Description:       {desc_meta.get('description') or '(empty)'}")

        # Technical Metadata
        tech_meta: Dict[str, Any] = workspace_info.get("technicalMetadata", {})
        logger.info("\nTECHNICAL METADATA")
        logger.info("-" * 80)

        # Quotas (convert bytes to GB)
        archive_quota_gb = tech_meta.get("archiveQuota", 0) / (1024**3)
        disk_quota_gb = tech_meta.get("diskQuota", 0) / (1024**3)
        pub_quota_gb = tech_meta.get("publicationQuota", 0) / (1024**3)

        logger.info(
            f"Archive Quota:     "
            f"{archive_quota_gb:.2f} GB ({tech_meta.get('archiveQuota')} bytes)"
        )
        logger.info(
            f"Disk Quota:        "
            f"{disk_quota_gb:.2f} GB ({tech_meta.get('diskQuota')} bytes)"
        )
        logger.info(
            f"Publication Quota: "
            f"{pub_quota_gb:.2f} GB ({tech_meta.get('publicationQuota')} bytes)"
        )

        # Support Features
        logger.info(
            f"\nAI Support:        "
            f"{'Enabled' if tech_meta.get('aiSupport') else 'Disabled'}"
        )
        logger.info(
            f"Git Support:       "
            f"{'Enabled' if tech_meta.get('gitSupport') else 'Disabled'}"
        )
        logger.info(
            f"WebDAV Support:    "
            f"{'Enabled' if tech_meta.get('webdavSupport') else 'Disabled'}"
        )

        # Dataset Settings
        logger.info(
            f"\nDefault Responsible:     "
            f"{tech_meta.get('defaultDatasetResponsibleEmail')}"
        )
        logger.info(
            f"Default Retention:       "
            f"{tech_meta.get('defaultDatasetRetentionPeriod')} years"
        )

        # Optional Collections
        logger.info(
            f"\nCategory Assignments:    "
            f"{tech_meta.get('categoryAssignments') or 'None'}"
        )
        logger.info(
            f"Curator Pre-Assignments: "
            f"{tech_meta.get('curatorPreAssignments') or 'None'}"
        )
        logger.info(
            f"Subcurator Assignments:  "
            f"{tech_meta.get('subcuratorPreAssignments') or 'None'}"
        )
        logger.info(f"Ontologies:              {tech_meta.get('ontologies') or 'None'}")
        logger.info(
            f"Ontology Collections:    {tech_meta.get('ontologyCollections') or 'None'}"
        )

        # Dataset IDs
        dataset_ids = workspace_info.get("datasetIds")
        logger.info(
            f"\nDataset IDs:       {dataset_ids if dataset_ids else 'No datasets yet'}"
        )

        logger.info("\n" + "=" * 80)

    def create_dataset(
        self,
        title: str,
        description: str,
        api_url: str = "https://test.radar-service.eu/radar/api/",
    ) -> Optional[str]:
        """Create a new dataset in a workspace.

        Args:
            workspace_id: RADAR workspace ID
            title: Dataset title
            description: Dataset description
            api_url: RADAR API base URL

        Returns:
            Dataset ID if successful, None otherwise

        """
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = f"{api_url}workspaces/{self.workspace_id}/datasets"

        # RADAR erwartet das vollständige DataCite-Format
        current_year = datetime.now().year

        payload = {
            "id": None,
            "parentId": self.workspace_id,
            "createdDate": None,
            "lastModifiedDate": None,
            "hasChildren": False,
            "state": "PENDING",
            "uploadUrl": None,
            "technicalMetadata": {
                "retentionPeriod": 10,
                "responsibleEmail": "jonas.dressner@kit.edu",
                "numberOfPendingNotificationMailsSent": 0,
                "categoryAssignments": None,
                "schema": {"key": "RDDM", "version": "9.2"},
            },
            "descriptiveMetadata": {
                "identifier": None,
                "alternateIdentifiers": None,
                "relatedIdentifiers": None,
                "creators": {
                    "creator": [
                        {
                            "creatorName": "ASSAS Project Team",
                            # "creatorAffiliation": "Scientist",
                        }
                    ]
                },
                "contributors": None,
                "title": title,
                "additionalTitles": None,
                "descriptions": {
                    "description": [
                        {"descriptionValue": description, "descriptionType": "ABSTRACT"}
                    ]
                }
                if description
                else None,
                "keywords": None,
                "publishers": None,
                "productionYear": str(current_year),
                "publicationYear": None,
                "language": "ENG",
                "subjectAreas": None,
                "resource": {
                    "resourceType": "DATASET",
                    "resourceTypeGeneral": "DATASET",
                },
                "geoLocations": None,
                "dataSources": None,
                "software": None,
                "processing": None,
                "rights": None,
                "rightsHolders": None,
                "relatedInformations": None,
            },
        }

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        logger.info(f"Creating dataset in workspace {self.workspace_id}")
        logger.debug(f"Dataset payload: {payload}")

        try:
            response = self.session.post(url, json=payload, headers=headers)

            if response.status_code in [200, 201]:
                dataset_data: Dict[str, Any] = response.json()
                dataset_id = dataset_data.get("id")
                logger.info(f"Successfully created dataset: {dataset_id}")
                logger.debug(f"Dataset data: {dataset_data}")
                return dataset_id
            else:
                logger.error(
                    f"Failed to create dataset: "
                    f"{response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Error creating dataset: {e}")
            return None

    def get_workspace_info(
        self,
        api_url: str = "https://test.radar-service.eu/radar/api/",
    ) -> Optional[Dict[str, Any]]:
        """Get workspace information using OAuth token.

        Args:
            workspace_id: RADAR workspace ID (wkbts39vbvn0gpu1)
            api_url: RADAR API base URL

        Returns:
            Workspace information dictionary or None

        """
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = f"{api_url}/workspaces/{self.workspace_id}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        logger.info(f"Fetching workspace: {self.workspace_id}")

        try:
            response = self.session.get(url, headers=headers)

            if response.status_code == 200:
                workspace_data = response.json()
                logger.info(f"Successfully retrieved workspace {self.workspace_id}")
                logger.debug(f"Workspace data: {workspace_data}")
                return workspace_data
            else:
                logger.error(
                    f"Failed to get workspace: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching workspace: {e}")
            return None

    def upload_file_via_webdav(
        self,
        dataset_id: str,
        file_path: str,
        remote_filename: Optional[str] = None,
        webdav_url: str = "https://test.radar-service.eu/webdav",
    ) -> bool:
        """Upload a file to a dataset using WebDAV.

        Args:
            dataset_id: Dataset ID
            file_path: Local file path to upload
            remote_filename: Optional remote filename (default: use local filename)
            webdav_url: WebDAV base URL

        Returns:
            True if successful, False otherwise

        """
        file_path_obj = Path(file_path)

        if not file_path_obj.exists():
            logger.error(f"File does not exist: {file_path}")
            return False

        filename = remote_filename or file_path_obj.name
        webdav_path = (
            f"{webdav_url}/workspaces/{self.workspace_id}/"
            f"datasets/{dataset_id}/{filename}"
        )

        logger.info(f"Uploading file via WebDAV: {file_path} -> {webdav_path}")
        logger.info(f"File size: {file_path_obj.stat().st_size / 1024:.2f} KB")

        try:
            with open(file_path, "rb") as f:
                response = self.session.put(
                    webdav_path,
                    data=f,
                    auth=HTTPBasicAuth(self.username, self.password),
                )

            if response.status_code in [200, 201, 204]:
                logger.info(f"Successfully uploaded file via WebDAV: {filename}")
                return True
            else:
                logger.error(
                    f"Failed to upload file via WebDAV: "
                    f"{response.status_code} - {response.text}"
                )
                return False

        except Exception as e:
            logger.error(f"Error uploading file via WebDAV: {e}")
            return False

    def get_dataset_files(
        self,
        dataset_id: str,
        api_url: str = "https://test.radar-service.eu/radar/api/",
    ) -> Optional[Dict[str, Any]]:
        """Get list of files in a dataset.

        Args:
            dataset_id: Dataset ID
            api_url: RADAR API base URL

        Returns:
            File information or None

        """
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = f"{api_url}workspaces/{self.workspace_id}/datasets/{dataset_id}/files"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        logger.info(f"Fetching files for dataset: {dataset_id}")

        try:
            response = self.session.get(url, headers=headers)

            if response.status_code == 200:
                files_data = response.json()
                logger.info(f"Successfully retrieved files for dataset {dataset_id}")
                logger.debug(f"Files data: {files_data}")
                return files_data
            else:
                logger.error(
                    f"Failed to get files: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching files: {e}")
            return None

    def create_and_upload_test_dataset_via_webdav(self) -> bool:
        """Create a test dataset and upload a test file via WebDAV."""
        # Step 1: Create test dataset
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Creating test dataset")
        logger.info("=" * 80)

        dataset_id = self.create_dataset(
            title="ASSAS Test Dataset WebDAV",
            description="Test dataset created via RADAR API with WebDAV file upload",
        )

        if not dataset_id:
            logger.error("Failed to create dataset")
            return False

        logger.info(f"Dataset created: {dataset_id}")

        # Step 2: Create a test file
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Creating test file")
        logger.info("=" * 80)

        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("ASSAS RADAR Test File - WebDAV Upload\n")
            f.write("=" * 60 + "\n")
            f.write(f"Created: {datetime.now().isoformat()}\n")
            f.write(f"Workspace ID: {self.workspace_id}\n")
            f.write(f"Dataset ID: {dataset_id}\n")
            f.write("\n")
            f.write("This is a test file uploaded via WebDAV to RADAR4KIT.\n")
            f.write("It demonstrates the WebDAV integration with the ASSAS database.\n")
            test_file_path = f.name

        logger.info(f"Test file created: {test_file_path}")

        # Step 3: Upload file via WebDAV
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Uploading file via WebDAV")
        logger.info("=" * 80)

        success = self.upload_file_via_webdav(
            dataset_id=dataset_id,
            file_path=test_file_path,
            remote_filename="assas_test_data_webdav.txt",
        )

        # Step 4: Verify dataset files
        if success:
            logger.info("\n" + "=" * 80)
            logger.info("STEP 4: Verifying dataset files")
            logger.info("=" * 80)

            files_info = self.get_dataset_files(dataset_id=dataset_id)

            if files_info:
                logger.info(f"Files in dataset: {files_info}")

        # Cleanup
        Path(test_file_path).unlink()
        logger.info(f"\nCleaned up test file: {test_file_path}")

        logger.info("\n" + "=" * 80)
        logger.info(f"{'Test completed successfully!' if success else 'Test failed!'}")
        logger.info("=" * 80)

        workspace_info = self.get_workspace_info()

        if workspace_info:
            self.plot_workspace_properties(workspace_info)
        else:
            logger.error("Could not access workspace")

        return success

    def access_assas_workspace(self) -> None:
        """Access ASSAS workspace."""
        dataset_id = self.create_dataset(
            title="ASSAS Test Dataset",
            description=(
                "Test dataset created via RADAR API for ASSAS database integration"
            ),
        )

        if not dataset_id:
            logger.error("Failed to create dataset")
            return

        logger.info(f"Dataset created: {dataset_id}. Accessing workspace info...")
        workspace_info = self.get_workspace_info()

        if workspace_info:
            self.plot_workspace_properties(workspace_info)
        else:
            logger.error("Could not access workspace")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="RADAR4KIT Handler CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )

    parser.add_argument(
        "--action",
        type=str,
        default="workspace",
        choices=["workspace", "webdav", "both"],
        help="Action to perform: workspace info, webdav test, or both",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    radar_user = os.getenv("RADAR_USERNAME")
    radar_password = os.getenv("RADAR_PASSWORD")
    client_id = os.getenv("RADAR_CLIENT_ID")
    client_secret = os.getenv("RADAR_CLIENT_SECRET")
    workspace_id = os.getenv("RADAR_WORKSPACE_ID")

    if not all([radar_user, radar_password, client_id, client_secret, workspace_id]):
        logger.error("Missing required environment variables:")
        logger.error("  RADAR_USERNAME, RADAR_PASSWORD, RADAR_CLIENT_ID,")
        logger.error("  RADAR_CLIENT_SECRET, RADAR_WORKSPACE_ID")
        exit(1)

    oauth_client = RadarOAuthClient(
        client_id=client_id,
        client_secret=client_secret,
        username=radar_user,
        password=radar_password,
        workspace_id=workspace_id,
        redirect_url="https://assas-horizon-euratom.eu/",
    )

    if args.action in ["workspace", "both"]:
        oauth_client.access_assas_workspace()

    if args.action in ["webdav", "both"]:
        oauth_client.create_and_upload_test_dataset_via_webdav()
