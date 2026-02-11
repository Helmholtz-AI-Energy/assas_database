"""RADAR4KIT OAuth2 Client and API Handler for ASSAS Database Integration."""

import requests
import logging
import tempfile
import argparse
import pandas as pd

from datetime import datetime
from typing import Optional, Dict, Any

from pathlib import Path
from requests.auth import HTTPBasicAuth

from assasdb import (
    AssasDocumentFileStatus,
    AssasDatabaseManager,
    AssasDatabaseHandler,
    require_env,
    find_env_file,
)

logger = logging.getLogger(__name__)

RADAR_DATASET_ID_FIELD = "radar_dataset_id"


def _api_base(url: str) -> str:
    """Ensure base URL ends with exactly one slash."""
    return url.rstrip("/") + "/"


def _clean_id(value: object) -> Optional[str]:
    """Return a usable id string or None (handles pandas NaN -> None)."""
    try:
        # pandas-safe NaN check (also catches None-like)
        if pd.isna(value):
            return None
    except Exception:
        pass

    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def load_env() -> dict[str, str]:
    """Load required env vars (from .env if present) and return as dict."""
    env_path = find_env_file()
    return require_env(
        env_path=env_path,
        logger=logger,
        keys=[
            # ASSAS / Mongo
            "CONNECTIONSTRING",
            "BACKUP_DIRECTORY",
            "MONGO_DB_NAME",
            # RADAR
            "RADAR_USERNAME",
            "RADAR_PASSWORD",
            "RADAR_CLIENT_ID",
            "RADAR_CLIENT_SECRET",
            "RADAR_WORKSPACE_ID",
            "RADAR_OAUTH_REDIRECT_URL",
            "RADAR_OAUTH_URL",
            "RADAR_API_URL",
        ],
    )


class RadarOAuthClient:
    """OAuth2 Client for RADAR4KIT API access."""

    def __init__(
        self,
        database_manager: AssasDatabaseManager,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        username: str,
        password: str,
        workspace_id: str,
        oauth_url: str,
        radar_api: str,
    ) -> None:
        """Initialize OAuth client.

        Args:
            database_manager: ASSAS Database Manager instance
            client_id: OAuth Client ID (assas-client)
            client_secret: OAuth Client Secret
            redirect_url: Redirect URL after authentication
            username: Optional username for OAuth2
            password: Optional password for OAuth2
            workspace_id: Workspace ID for RADAR
            oauth_url: Token endpoint URL
            radar_api: Base URL for RADAR API

        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        self.username = username
        self.password = password
        self.workspace_id = workspace_id
        self.oauth_url = oauth_url
        self.radar_api = _api_base(radar_api)

        self.access_token = None
        self.session = requests.Session()

        logger.info("Initialized RadarOAuthClient.")
        logger.info(f"Client ID: {self.client_id}.")
        logger.info(f"Username: {self.username}.")
        logger.info(f"Redirect URL: {self.redirect_url}.")
        logger.info(f"OAuth URL: {self.oauth_url}.")
        logger.info(f"Workspace ID: {self.workspace_id}.")
        logger.info(f"RADAR API: {self.radar_api}.")

        self.manager = database_manager
        self.dataframe = database_manager.get_all_database_entries()
        logger.info(
            f"Loaded dataframe with {len(self.dataframe)} entries from ASSAS database."
        )

    def persist_radar_dataset_id(
        self,
        upload_uuid: str,
        system_uuid: str,
        dataset_id: str,
    ) -> None:
        """Persist the mapping between ASSAS upload_uuid and RADAR dataset_id."""
        # This is a placeholder implementation. You should implement the actual
        # persistence logic according to your database schema and requirements.
        logger.info(
            f"Persisting mapping: \n"
            f"upload_uuid={upload_uuid}, system_uuid={system_uuid} \n"
            f"-> dataset_id={dataset_id}"
        )

        # Example: Update the ASSAS database entry with the RADAR dataset ID
        # self.database_manager.update_dataset_id(upload_uuid, dataset_id)
        post_doc = {
            f"{RADAR_DATASET_ID_FIELD}": dataset_id,
            f"{RADAR_DATASET_ID_FIELD}_updated_at": datetime.utcnow().isoformat(
                timespec="seconds"
            )
            + "Z",
        }

        result = self.manager.database_handler.update_file_document_by_uuid(
            uuid=system_uuid,
            update=post_doc,
        )

        if result.modified_count == 1:
            logger.info(f"Successfully updated database for system_uuid={system_uuid}.")
        else:
            logger.error(
                f"Failed to update database for system_uuid={system_uuid}. "
                f"Modified count: {result.modified_count}."
            )

        return None

    def delete_radar_dataset_id(
        self,
        *,
        system_uuid: str,
        delete_updated_at: bool = True,
    ) -> bool:
        """Delete (unset) the RADAR dataset id field from MongoDB.

        Returns True if a document was matched (field may or may not have existed),
        False if no document matched the given system_uuid.
        """
        system_uuid_clean = _clean_id(system_uuid)
        if not system_uuid_clean:
            logger.error(
                "Cannot delete %s: system_uuid is empty/NaN.", RADAR_DATASET_ID_FIELD
            )
            return False

        unset_doc: Dict[str, Any] = {RADAR_DATASET_ID_FIELD: ""}
        if delete_updated_at:
            unset_doc[f"{RADAR_DATASET_ID_FIELD}_updated_at"] = ""

        # Use $unset directly
        # (update_file_document_by_uuid likely wraps $set internally).
        result = self.manager.database_handler.file_collection.update_one(
            {"system_uuid": str(system_uuid_clean)},
            {"$unset": unset_doc},
        )

        if getattr(result, "matched_count", 0) == 0:
            logger.error(
                "No document found for system_uuid=%s; nothing unset.",
                system_uuid_clean,
            )
            return False

        logger.info(
            "Unset %s for system_uuid=%s (matched=%s, modified=%s).",
            RADAR_DATASET_ID_FIELD,
            system_uuid_clean,
            getattr(result, "matched_count", None),
            getattr(result, "modified_count", None),
        )
        return True

    def delete_for_all(self) -> int:
        """Delete (unset) the RADAR dataset id field from all documents in MongoDB.

        Returns:
            The count of documents that were matched
                (field may or may not have existed).

        """
        unset_doc = {
            RADAR_DATASET_ID_FIELD: "",
            f"{RADAR_DATASET_ID_FIELD}_updated_at": "",
        }

        result = self.manager.database_handler.file_collection.update_many(
            {RADAR_DATASET_ID_FIELD: {"$exists": True}},
            {"$unset": unset_doc},
        )

        logger.info(
            "Unset %s for all documents where it existed (matched=%s, modified=%s).",
            RADAR_DATASET_ID_FIELD,
            getattr(result, "matched_count", None),
            getattr(result, "modified_count", None),
        )
        return getattr(result, "matched_count", 0)

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

        logger.debug("Requesting OAuth access token (payload redacted).")

        try:
            response = requests.post(
                self.oauth_url,
                json=payload,
                headers=headers,
            )

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get("access_token")
                logger.info("Successfully obtained access token.")
                return self.access_token
            else:
                logger.error(
                    f"Failed to get token: {response.status_code} - {response.text}."
                )
                return None

        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None

    def _build_radar_dataset_payload_from_row(
        self,
        dataframe_row: object,
        *,
        dataset_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build a RADAR dataset payload (DataCite-like) from one ASSAS dataframe row.

        Args:
            dataframe_row: A row from the ASSAS dataframe (pandas Series or dict-like)
            dataset_id: Optional dataset ID (for updates; None for create)

        Returns:
            A dictionary representing the RADAR dataset payload.

        """
        current_year = datetime.now().year

        payload: Dict[str, Any] = {
            "id": dataset_id,  # None for create, existing id for update
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
                        }
                    ]
                },
                "contributors": None,
                "title": dataframe_row.get("meta_name", ""),
                "additionalTitles": None,
                "descriptions": {
                    "description": [
                        {
                            "descriptionValue": dataframe_row.get(
                                "meta_description", ""
                            ),
                            "descriptionType": "ABSTRACT",
                        }
                    ]
                }
                if dataframe_row.get("meta_description")
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
        return payload

    def create_dataset_from_dataframe_row(
        self,
        dataframe_row: object,
        api_url: Optional[str] = None,
    ) -> Optional[str]:
        """Create a new dataset in a workspace."""
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        base = _api_base(api_url or self.radar_api)
        url = f"{base}workspaces/{self.workspace_id}/datasets"

        payload = self._build_radar_dataset_payload_from_row(
            dataframe_row, dataset_id=None
        )

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

    def update_dataset_from_dataframe_row(
        self,
        dataframe_row: object,
        *,
        dataset_id: str,
        api_url: Optional[str] = None,
    ) -> bool:
        """Update an existing dataset's metadata from a dataframe row."""
        dataset_id_clean = _clean_id(dataset_id)
        if not dataset_id_clean:
            logger.error("Refusing to update: dataset_id is empty/NaN: %r", dataset_id)
            return False

        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return False

        base = _api_base(api_url or self.radar_api)
        urls_to_try = [
            f"{base}workspaces/{self.workspace_id}/datasets/{dataset_id_clean}",
            # Some APIs use dataset endpoints without workspace prefix;
            # try as fallback on 404.
            f"{base}datasets/{dataset_id_clean}",
        ]

        payload = self._build_radar_dataset_payload_from_row(
            dataframe_row,
            dataset_id=dataset_id_clean,
        )

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        for url in urls_to_try:
            logger.info(
                "Updating dataset_id=%s in workspace %s (PATCH %s)",
                dataset_id_clean,
                self.workspace_id,
                url,
            )

            try:
                resp = self.session.patch(url, json=payload, headers=headers)
                logger.debug("PATCH %s -> %s %s", url, resp.status_code, resp.text)

                if resp.status_code in [200, 204]:
                    logger.info(
                        "Successfully updated dataset "
                        f"via PATCH: {dataset_id_clean} (URL: {url})"
                    )
                    return True

                # If first URL 404s, try the next candidate URL
                if resp.status_code == 404:
                    continue

                # Don't fallback to PUT (your endpoint returns CSRF errors on PUT)
                logger.error(
                    f"Failed to update dataset via PATCH: "
                    f"{resp.status_code} - {resp.text}"
                )
                return False

            except Exception as e:
                logger.error(f"Error updating dataset_id={dataset_id_clean}: {e}")
                return False

        logger.error(
            f"Failed to update dataset_id={dataset_id_clean}: "
            f"endpoint not found (404) for all tried URLs."
        )
        return False

    def create_datasets_from_dataframe(
        self,
        *,
        api_url: Optional[str] = None,
        only_uuids: Optional[set[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
        update: bool = False,
    ) -> list[dict[str, str]]:
        """Create datasets for dataframe rows (one dataset per row).

        If radar_dataset_id exists and update=True, update metadata instead of creating.
        Returns list of mappings: {system_upload_uuid, dataset_id}.
        """
        results: list[dict[str, str]] = []
        created = 0
        considered = 0  # counts rows that pass filtering (uuid/status)

        # Acquire token once
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return results

        for _, row in self.dataframe.iterrows():
            upload_uuid = _clean_id(row.get("system_upload_uuid", "")) or ""

            # Optional filtering
            if only_uuids and upload_uuid not in only_uuids:
                continue

            # Optional status filtering (skip non-VALID)
            if "system_status" in row:
                try:
                    status = AssasDocumentFileStatus(row.get("system_status"))
                    if status != AssasDocumentFileStatus.VALID:
                        logger.info(
                            f"Skipping upload_uuid={upload_uuid} "
                            f"due to system_status={status}"
                        )
                        continue
                except Exception:
                    pass

            # Apply limit AFTER filtering (matches CLI help)
            considered += 1
            if limit is not None and considered > int(limit):
                logger.info("Reached limit=%s (after filtering); stopping.", limit)
                break

            system_uuid = _clean_id(row.get("system_uuid"))

            # Check whether radar_dataset_id already exists (prefer fresh DB state)
            existing_dataset_id: Optional[str] = None
            try:
                if system_uuid:
                    document: dict[str, Any] = (
                        self.manager.database_handler.get_file_document_by_uuid(
                            uuid=system_uuid
                        )
                    )
                    if document:
                        existing_dataset_id = _clean_id(
                            document.get(RADAR_DATASET_ID_FIELD)
                        )
            except Exception as e:
                logger.warning(
                    f"Could not fetch existing document for "
                    f"system_uuid={system_uuid} to check {RADAR_DATASET_ID_FIELD}: {e}"
                )

            if not existing_dataset_id:
                existing_dataset_id = _clean_id(row.get(RADAR_DATASET_ID_FIELD))

            # Existing dataset_id path
            if existing_dataset_id:
                if update:
                    if dry_run:
                        logger.info(
                            "[DRY-RUN] Would update metadata "
                            f"for upload_uuid={upload_uuid}"
                            f"dataset_id={existing_dataset_id}.",
                        )
                        results.append(
                            {
                                "system_upload_uuid": upload_uuid,
                                "dataset_id": existing_dataset_id,
                            }
                        )
                        continue

                    ok = self.update_dataset_from_dataframe_row(
                        row,
                        dataset_id=existing_dataset_id,
                        api_url=api_url,
                    )
                    if ok:
                        results.append(
                            {
                                "system_upload_uuid": upload_uuid,
                                "dataset_id": existing_dataset_id,
                            }
                        )
                    else:
                        logger.error(
                            "Failed updating dataset "
                            f"metadata for upload_uuid={upload_uuid} "
                            f"dataset_id={existing_dataset_id}",
                        )
                    continue

                logger.info(
                    f"Skipping creation for upload_uuid={upload_uuid} "
                    f"because {RADAR_DATASET_ID_FIELD} "
                    f"already exists: {existing_dataset_id}."
                )
                results.append(
                    {
                        "system_upload_uuid": upload_uuid,
                        "dataset_id": existing_dataset_id,
                    }
                )
                continue

            # Create path (only when no existing dataset_id)
            if dry_run:
                logger.info(
                    "[DRY-RUN] Would create dataset for upload_uuid=%s.", upload_uuid
                )
                results.append({"system_upload_uuid": upload_uuid, "dataset_id": ""})
                created += 1
                continue

            dataset_id = self.create_dataset_from_dataframe_row(row, api_url=api_url)
            if not dataset_id:
                logger.error("Failed creating dataset for upload_uuid=%s", upload_uuid)
                continue

            # Persist mapping in MongoDB
            if system_uuid:
                self.persist_radar_dataset_id(
                    upload_uuid=upload_uuid,
                    system_uuid=system_uuid,
                    dataset_id=dataset_id,
                )
            else:
                logger.error(
                    f"Created dataset_id={dataset_id} for upload_uuid={upload_uuid} "
                    f"but cannot persist: missing system_uuid in dataframe row.",
                )

            results.append(
                {"system_upload_uuid": upload_uuid, "dataset_id": dataset_id}
            )
            logger.info(
                f"Created dataset_id={dataset_id} for upload_uuid={upload_uuid}."
            )
            created += 1

        return results

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
                logger.info(f"Successfully retrieved workspace {self.workspace_id}.")
                logger.debug(f"Workspace data: {workspace_data}.")
                return workspace_data
            else:
                logger.error(
                    f"Failed to get workspace: "
                    f"{response.status_code} - {response.text}."
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching workspace: {e}")
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
        logger.info("Accessing workspace info...")
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
        choices=["workspace", "webdav", "both", "from-db"],
        help="Action to perform",
    )
    parser.add_argument(
        "--uuid",
        action="append",
        default=[],
        help=(
            "When used with --action from-db: only create "
            "datasets for these system_upload_uuid(s).",
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "When used with --action from-db: "
            "create only the first N datasets (after filtering).",
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "When used with --action from-db: "
            "do not call RADAR, only log what would happen.",
        ),
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "When used with --action from-db: if radar_dataset_id already exists, "
            "update that dataset's metadata instead of creating a new one.",
        ),
    )
    parser.add_argument(
        "--delete-dataset-ids",
        action="store_true",
        default=False,
        help=(
            "Delete (unset) the RADAR dataset ID field from all documents in MongoDB. "
            "Use with caution! This does NOT delete datasets from RADAR, only the "
            "mapping field in MongoDB. Use --delete-dataset-ids --uuid <uuid> to only "
            "delete for specific system_uuid(s)."
        ),
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    env = load_env()

    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            connection_string=env["CONNECTIONSTRING"],
            backup_directory=env["BACKUP_DIRECTORY"],
            database_name=env["MONGO_DB_NAME"],
        )
    )

    oauth_client = RadarOAuthClient(
        database_manager=database_manager,
        client_id=env["RADAR_CLIENT_ID"],
        client_secret=env["RADAR_CLIENT_SECRET"],
        username=env["RADAR_USERNAME"],
        password=env["RADAR_PASSWORD"],
        workspace_id=env["RADAR_WORKSPACE_ID"],
        redirect_url=env["RADAR_OAUTH_REDIRECT_URL"],
        oauth_url=env["RADAR_OAUTH_URL"],
        radar_api=env["RADAR_API_URL"],
    )

    if args.action in ["workspace", "both"]:
        oauth_client.access_assas_workspace()

    if args.action in ["webdav", "both"]:
        oauth_client.create_and_upload_test_dataset_via_webdav()

    if args.action == "from-db":
        only = set(args.uuid) if args.uuid else None
        oauth_client.create_datasets_from_dataframe(
            only_uuids=only,
            limit=args.limit,
            dry_run=args.dry_run,
            update=args.update,
        )

    if args.delete_dataset_ids:
        oauth_client.delete_for_all()
