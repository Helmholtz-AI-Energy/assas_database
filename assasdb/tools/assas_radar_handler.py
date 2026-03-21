"""RADAR4KIT OAuth2 Client and API Handler for ASSAS Database Integration."""

import os
import requests
import logging
import argparse
import pandas as pd
import xml.etree.ElementTree as ET

from enum import IntEnum
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Literal, Union
from collections.abc import Mapping

from assasdb import (
    AssasDocumentFileStatus,
    AssasDatabaseManager,
    AssasDatabaseHandler,
    require_env,
    find_env_file,
)

logger = logging.getLogger(__name__)

RADAR_DATASET_ID_FIELD = "radar_dataset_id"

NS_DATASET = "http://radar-service.eu/schemas/descriptive/radar/v09/radar-dataset"
NS_ELEMENTS = "http://radar-service.eu/schemas/descriptive/radar/v09/radar-elements"

XML_TEMPLATE_DIR = "xml"

USER_TEMPLATE_MAPPING = {
    "Joan": os.path.join(XML_TEMPLATE_DIR, "radar_dataset_template_joan.xml"),
    "Marcello": os.path.join(XML_TEMPLATE_DIR, "radar_dataset_template_marcello.xml"),
    "Anastasia": os.path.join(XML_TEMPLATE_DIR, "radar_dataset_template_anastasia.xml"),
    "Jure": os.path.join(XML_TEMPLATE_DIR, "radar_dataset_template_jure.xml"),
}

DEFAULT_TEMPLATE = os.path.join(XML_TEMPLATE_DIR, "radar_dataset_template.xml")

RadarFormat = Literal["xml", "json"]
RowLike = Union[pd.Series, Mapping[str, Any]]


class RadarReturnCode(IntEnum):
    """HTTP return codes used by RADAR endpoints."""

    OK = 200  # Request succeeded (resource returned or action completed)
    CREATED = 201  # Resource created successfully
    NO_CONTENT = 204  # Request succeeded but no content returned

    @property
    def meaning(self) -> str:
        """Human-readable meaning of the return code."""
        if self is RadarReturnCode.OK:
            return "OK - request succeeded"
        if self is RadarReturnCode.CREATED:
            return "Created - resource created"
        if self is RadarReturnCode.NO_CONTENT:
            return "No Content - request succeeded but no content returned"
        return "Unknown"


def _row_get(row: object, key: str, default: object = None) -> object:
    """Safe accessor for dataframe rows / dict-like objects.

    Works with:
      - pandas.Series (.get)
      - dict / Mapping (.get)
      - falls back to __getitem__
    """
    try:
        get = getattr(row, "get", None)
        if callable(get):
            return get(key, default)
    except Exception:
        pass

    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return default


def _xml_local_name(tag: str) -> str:
    """Extract local name from XML tag, ignoring namespace."""
    # Handles "{namespace}Tag" -> "Tag"
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def remove_empty_fields(d: object) -> object:
    """Recursively remove empty fields from dicts/lists (None, '', [], {})."""
    if isinstance(d, dict):
        return {
            k: remove_empty_fields(v)
            for k, v in d.items()
            if v not in [None, "", [], {}]
        }
    elif isinstance(d, list):
        return [remove_empty_fields(x) for x in d if x not in [None, "", [], {}]]
    else:
        return d


def _build_element(tag: str, value: object, ns: Optional[str] = None) -> ET.Element:
    """Build an XML element with optional namespace."""
    if ns:
        tag = f"{{{ns}}}{tag}"
    el = ET.Element(tag)
    if value is None:
        return el
    if isinstance(value, dict):
        for k, v in value.items():
            # Use elements namespace for children
            child = _build_element(k, v, NS_ELEMENTS)
            el.append(child)
        return el
    if isinstance(value, (list, tuple)):
        for item in value:
            child = _build_element("item", item, NS_ELEMENTS)
            el.append(child)
        return el
    el.text = str(value)
    return el


def _dict_to_radar_xml(payload: dict) -> ET.Element:
    """Convert your payload dict to a RADAR-compliant XML Element with namespaces."""
    # Register namespaces for pretty output
    ET.register_namespace("rd", NS_DATASET)
    ET.register_namespace("", NS_ELEMENTS)

    # Root element with ns2 prefix
    root = ET.Element(f"{{{NS_DATASET}}}radarDataset")
    # Add children from payload (in elements namespace)
    for k, v in payload.get("descriptiveMetadata", {}).items():
        child = _build_element(k, v, NS_ELEMENTS)
        root.append(child)

    return root


def _dict_to_xml_element(tag: str, value: object) -> ET.Element:
    """Convert dict/list/scalar into a very simple XML structure.

    This is a generic mapping and may need adjustment to RADAR's expected XML schema.
    """
    el = ET.Element(tag)

    if value is None:
        return el

    if isinstance(value, dict):
        for k, v in value.items():
            child = _dict_to_xml_element(str(k), v)
            el.append(child)
        return el

    if isinstance(value, (list, tuple)):
        # Repeated "item" elements
        for item in value:
            child = _dict_to_xml_element("item", item)
            el.append(child)
        return el

    # scalar
    el.text = str(value)
    return el


def _xml_element_to_obj(el: ET.Element) -> Union[dict[str, Any], str]:
    """Convert an XML element into a dict or string.

    Cases:
    - leaf -> text
    - repeated child tags -> list
    - otherwise -> dict
    """
    children = list(el)
    if not children:
        return (el.text or "").strip()

    grouped: dict[str, list[Any]] = {}
    for c in children:
        k = _xml_local_name(c.tag)
        grouped.setdefault(k, []).append(_xml_element_to_obj(c))

    out: dict[str, Any] = {}
    for k, vals in grouped.items():
        out[k] = vals[0] if len(vals) == 1 else vals
    return out


def _best_effort_parse_body(resp: requests.Response) -> Union[dict[str, Any], str]:
    """Parse response as JSON when it looks like JSON, otherwise try XML.

    Returns dict for JSON/XML, or raw text as fallback.
    """
    ctype = (resp.headers.get("Content-Type") or "").lower()
    text = resp.text or ""

    # Prefer JSON when indicated or body looks like JSON
    if (
        "application/json" in ctype
        or text.lstrip().startswith("{")
        or text.lstrip().startswith("[")
    ):
        try:
            data = resp.json()
            if isinstance(data, dict):
                return data
            return {"data": data}
        except Exception:
            return text

    # Try XML
    try:
        root = ET.fromstring(text.encode("utf-8") if isinstance(text, str) else text)
        obj = _xml_element_to_obj(root)
        if isinstance(obj, dict):
            return obj
        return {"data": obj}
    except Exception:
        return text


def _find_id_in_parsed(parsed: Union[dict[str, Any], str]) -> Optional[str]:
    """Try to extract an 'id' field from parsed JSON/XML responses."""
    if isinstance(parsed, dict):
        # common case: {"id": "..."}
        if "id" in parsed and isinstance(parsed["id"], (str, int)):
            return str(parsed["id"])

        # search recursively for first "id"
        stack: list[Any] = [parsed]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                if "id" in cur and isinstance(cur["id"], (str, int)):
                    return str(cur["id"])
                stack.extend(cur.values())
            elif isinstance(cur, list):
                stack.extend(cur)
        return None

    # raw text fallback (XML-ish)
    if isinstance(parsed, str):
        # very small heuristic: <id>...</id> (ignoring namespaces)
        try:
            root = ET.fromstring(parsed.encode("utf-8"))
            for node in root.iter():
                if (
                    _xml_local_name(node.tag).lower() == "id"
                    and (node.text or "").strip()
                ):
                    return (node.text or "").strip()
        except Exception:
            return None

    return None


def _api_base(url: str) -> str:
    """Ensure base URL ends with exactly one slash."""
    return url.rstrip("/") + "/"


def _clean_id(value: object) -> Optional[str]:
    """Return a usable id string or None (handles pandas NaN -> None)."""
    if value is None:
        return None

    try:
        # pandas-safe NaN check (also catches None-like)
        if pd.isna(value):  # type: ignore[arg-type]
            return None
    except Exception:
        pass

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
        radar_format: RadarFormat = "xml",
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
            radar_format: RadarFormat = "xml",

        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        self.username = username
        self.password = password
        self.workspace_id = workspace_id
        self.oauth_url = oauth_url
        self.radar_api = _api_base(radar_api)

        self.radar_format: RadarFormat = (
            radar_format if radar_format in ("xml", "json") else "xml"
        )

        self.access_token = None
        self.session = requests.Session()

        logger.info("Initialized RadarOAuthClient.")
        logger.info(f"Client ID: {self.client_id}.")
        logger.info(f"Username: {self.username}.")
        logger.info(f"Redirect URL: {self.redirect_url}.")
        logger.info(f"OAuth URL: {self.oauth_url}.")
        logger.info(f"Workspace ID: {self.workspace_id}.")
        logger.info(f"RADAR API: {self.radar_api}.")
        logger.info("RADAR format (default): %s", self.radar_format)

        self.manager = database_manager
        self.dataframe: Optional[pd.DataFrame] = None
        logger.info("Deferred MongoDB dataframe loading (streaming mode).")
        # self.dataframe = database_manager.get_all_database_entries()
        # logger.info(
        #    f"Loaded dataframe with {len(self.dataframe)} entries from ASSAS database."
        # )

    def _accept_header(self) -> str:
        return "application/json" if self.radar_format == "json" else "application/xml"

    def _content_type_header(self) -> str:
        return "application/json" if self.radar_format == "json" else "application/xml"

    def _request_headers(
        self,
        *,
        with_auth: bool = True,
        with_body: bool = False,
        format_override: Optional[RadarFormat] = None,
    ) -> dict[str, str]:
        fmt: RadarFormat = format_override or self.radar_format
        headers: dict[str, str] = {
            "Accept": "application/json" if fmt == "json" else "application/xml"
        }

        if with_auth and self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        if with_body:
            headers["Content-Type"] = (
                "application/json" if fmt == "json" else "application/xml"
            )

        return headers

    def _encode_payload(
        self,
        payload: dict[str, Any],
        *,
        format_override: Optional[RadarFormat] = None,
    ) -> dict[str, Any]:
        """Encode the payload according to the specified format (JSON or XML).

        Return kwargs for requests: either {"json": payload} or {"data": xml_bytes}.
        """
        fmt: RadarFormat = format_override or self.radar_format

        if fmt == "json":
            return {"json": payload}

        # XML default
        root = _dict_to_xml_element("request", payload)
        xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        return {"data": xml_bytes}

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

            if response.status_code == RadarReturnCode.OK:
                token_data: Dict[str, Any] = response.json()
                self.access_token = token_data.get("access_token")
                logger.info(f"Successfully obtained access token. {self.access_token}")
                return self.access_token
            else:
                logger.error(
                    f"Failed to get token: {response.status_code} - {response.text}."
                )
                return None

        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None

    def _build_radar_dataset_payload_from_row_for_creation(
        self,
        dataframe_row: RowLike,
        *,
        dataset_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build the payload for creating a RADAR dataset from a dataframe row."""
        current_year = datetime.now().year
        production_year = str(current_year)

        user_info = _row_get(dataframe_row, "system_user_info", {}) or {}
        user_name: str = ""
        if isinstance(user_info, Mapping):
            user_name = str(user_info.get("name") or "").strip()
        else:
            # fallback if DB stored something unexpected (e.g., string)
            user_name = str(user_info).strip()

        if not user_name:
            user_name = "ASSAS Project Team"

        created_date = _row_get(
            dataframe_row,
            "system_date",
            datetime.now(timezone.utc).isoformat(),
        )

        meta_name = _row_get(dataframe_row, "meta_name", "") or ""
        meta_description = _row_get(dataframe_row, "meta_description", None)

        payload: Dict[str, Any] = {
            "id": dataset_id,
            "parentId": self.workspace_id,
            "createdDate": created_date,
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
                "creators": {"creator": [{"creatorName": user_name}]},
                "contributors": None,
                "title": meta_name,
                "additionalTitles": None,
                "descriptions": (
                    {
                        "description": [
                            {
                                "descriptionValue": str(meta_description),
                                "descriptionType": "ABSTRACT",
                            }
                        ]
                    }
                    if meta_description
                    else None
                ),
                "keywords": None,
                "publishers": None,
                "productionYear": production_year,
                "publicationYear": None,
                "language": None,
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

    def _build_radar_dataset_payload_from_row(
        self,
        dataframe_row: RowLike,
        *,
        dataset_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        current_year = datetime.now().year

        user_info = _row_get(dataframe_row, "system_user_info", {}) or {}
        user_name: str = ""
        if isinstance(user_info, Mapping):
            user_name = str(user_info.get("name") or "").strip()
        else:
            # fallback if DB stored something unexpected (e.g., string)
            user_name = str(user_info).strip()

        if not user_name:
            user_name = "ASSAS Project Team"

        created_date = _row_get(
            dataframe_row,
            "system_date",
            datetime.now(timezone.utc).isoformat(),
        )

        meta_name = _row_get(dataframe_row, "meta_name", "") or ""
        meta_description = _row_get(dataframe_row, "meta_description", None)

        # identifierValue: must match pattern and not be empty
        identifier_value = f"radar/{dataset_id}" if dataset_id else "radar/unknown"
        # year: must be 4 digits
        production_year = str(current_year)
        # language: must be lowercase ISO 639-3 code
        language_code = "eng"

        if not identifier_value or not production_year or len(production_year) != 4:
            logger.error("Invalid identifierValue or productionYear!")
            raise ValueError("Invalid identifierValue or productionYear")

        payload: Dict[str, Any] = {
            "id": dataset_id,
            "parentId": self.workspace_id,
            "createdDate": created_date,
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
                "creators": {"creator": [{"creatorName": user_name}]},
                "contributors": None,
                "title": meta_name,
                "additionalTitles": None,
                "descriptions": (
                    {
                        "description": [
                            {
                                "descriptionValue": str(meta_description),
                                "descriptionType": "ABSTRACT",
                            }
                        ]
                    }
                    if meta_description
                    else None
                ),
                "keywords": None,
                "publishers": None,
                "productionYear": production_year,
                "publicationYear": None,
                "language": language_code,
                "subjectAreas": {
                    "subjectArea": ["nuclear physics", "computer science"]
                },
                "resource": {
                    "resourceType": "DATASET",
                    "resourceTypeGeneral": "DATASET",
                },
                "geoLocations": None,
                "dataSources": None,
                "software": "ASTEC V3.1.2",
                "processing": None,
                "rights": None,
                "rightsHolders": None,
                "relatedInformations": None,
            },
        }
        return payload

    @staticmethod
    def _looks_like_server_expected_json(resp: requests.Response) -> bool:
        if resp.status_code != 400:
            return False
        txt = resp.text or ""
        return "Unexpected character ('<'" in txt and "expected a valid value" in txt

    def create_dataset_from_dataframe_row(
        self,
        dataframe_row: RowLike,
        api_url: Optional[str] = None,
    ) -> Optional[str]:
        """Create a new dataset in a workspace using JSON upload."""
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        base = _api_base(api_url or self.radar_api)
        url = f"{base}workspaces/{self.workspace_id}/datasets"

        payload = self._build_radar_dataset_payload_from_row_for_creation(
            dataframe_row, dataset_id=None
        )
        payload = remove_empty_fields(payload)

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        logger.info("JSON to upload:\n%s", payload)

        try:
            logger.info(f"POST {url} with headers={headers} and JSON body.")
            response = self.session.post(url, headers=headers, json=payload)

            if response.status_code in (RadarReturnCode.OK, RadarReturnCode.CREATED):
                parsed = _best_effort_parse_body(response)
                dataset_id = _find_id_in_parsed(parsed)
                logger.info(f"Successfully created dataset: {dataset_id}")
                logger.debug("Response parsed: %s", parsed)
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

    def get_dataset_metadata_xml(
        self,
        dataset_id: str,
    ) -> Optional[str]:
        """Fetch the metadata XML for a dataset from RADAR and log/print the structure.

        Returns the raw XML string if successful, or None on failure.
        """
        dataset_id_clean = _clean_id(dataset_id)
        if not dataset_id_clean:
            logger.error("Refusing to fetch: dataset_id is empty/NaN: %r", dataset_id)
            return None

        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = f"{self.radar_api}datasets/{dataset_id_clean}/metadata"
        headers = self._request_headers(
            with_auth=True, with_body=False, format_override="xml"
        )

        logger.info(f"Fetching metadata XML for dataset {dataset_id_clean} at {url}")

        try:
            resp = self.session.get(url, headers=headers)
            logger.info(
                "GET %s -> %s ct=%s | Response: %s",
                url,
                resp.status_code,
                resp.headers.get("Content-Type"),
                resp.text[:300],
            )
            if resp.status_code == RadarReturnCode.OK:
                logger.info(
                    "Successfully fetched metadata XML for dataset: %s",
                    dataset_id_clean,
                )
                logger.info("Full XML:\n%s", resp.text)
                # Optionally, pretty-print the XML structure
                try:
                    root = ET.fromstring(resp.content)
                    xml_str = ET.tostring(root, encoding="utf-8").decode("utf-8")
                    print(xml_str)
                except Exception as e:
                    logger.warning("Could not parse XML for pretty-print: %s", e)
                return resp.text
            else:
                logger.error(
                    "Failed to fetch metadata XML: %s - %s",
                    resp.status_code,
                    resp.text,
                )
                return None
        except Exception as e:
            logger.error("Dataset metadata fetch error (XML): %s", e)
            return None

    def _get_template_path_for_user(
        self,
        user_name: str,
    ) -> str:
        """Get the appropriate template path based on user name.

        Args:
            user_name: Name of the data generator/creator

        Returns:
            Template filename matching the user, or default template

        """
        # Check for exact match first
        for user, template in USER_TEMPLATE_MAPPING.items():
            if user.lower() in user_name.lower():
                logger.info(f"Using template '{template}' for user '{user_name}'")
                return template

        logger.info(
            f"No specific template found for user '{user_name}', "
            f"using default template '{DEFAULT_TEMPLATE}'"
        )
        return DEFAULT_TEMPLATE

    def _first_non_empty(self, row: RowLike, keys: list[str]) -> str:
        """Return the first non-empty value from the given row for the specified keys.

        Args:
            row: The data row to search.
            keys: List of keys to check in order.

        Returns:
            The first non-empty value as a string, or an empty string if none found.

        """
        for k in keys:
            v = _row_get(row, k, None)
            if v is not None and str(v).strip() and str(v).strip().lower() != "nan":
                return str(v).strip()
        return ""

    def _find_abstract_description_value(
        self, xml_root: ET.Element
    ) -> Optional[ET.Element]:
        """Return the element whose .text should be updated for ABSTRACT description.

        Supports both:
          1) <description descriptionType="Abstract">text</description>
          2) <description>
                <descriptionType>ABSTRACT
                </descriptionType>
                <descriptionValue>text
                </descriptionValue>
            </description>
        """
        # Shape 1: attribute on <description>
        for desc in xml_root.iter():
            if _xml_local_name(desc.tag) != "description":
                continue
            dtype_attr = (desc.attrib.get("descriptionType") or "").strip().upper()
            if dtype_attr == "ABSTRACT":
                return desc

        # Shape 2: nested descriptionType + descriptionValue
        for desc in xml_root.iter():
            if _xml_local_name(desc.tag) != "description":
                continue

            desc_type = ""
            desc_value_el: Optional[ET.Element] = None
            for child in list(desc):
                lname = _xml_local_name(child.tag)
                if lname == "descriptionType":
                    desc_type = (child.text or "").strip().upper()
                elif lname == "descriptionValue":
                    desc_value_el = child

            if desc_type == "ABSTRACT" and desc_value_el is not None:
                return desc_value_el

        # Fallback: first description/descriptionValue
        for node in xml_root.iter():
            if _xml_local_name(node.tag) in {"description", "descriptionValue"}:
                return node
        return None

    def update_radar_metadata_with_template(
        self,
        *,
        dataframe_row: RowLike,
        dataset_id: str,
        template_path: Optional[str] = None,
    ) -> bool:
        """Load the XML template, set the title, and upload to RADAR.

        Args:
            dataframe_row: Row from dataframe containing metadata
            dataset_id: RADAR dataset ID
            template_path: Optional explicit template path. If not provided,
                          will auto-select based on user name.

        """
        # Auto-select template based on user if not explicitly provided
        if template_path is None:
            user_info = _row_get(dataframe_row, "system_user_info", {}) or {}
            user_name: str = ""
            if isinstance(user_info, Mapping):
                user_name = str(user_info.get("name") or "").strip()
            else:
                user_name = str(user_info).strip()

            if not user_name:
                user_name = "ASSAS Project Team"

            template_path = self._get_template_path_for_user(user_name)

        current_dir = os.path.dirname(os.path.abspath(__file__))
        template_full_path = os.path.join(current_dir, template_path)

        # Parse the XML template
        try:
            tree = ET.parse(template_full_path)
            root = tree.getroot()
        except FileNotFoundError:
            logger.error(
                f"Template file not found: {template_full_path}. "
                f"Make sure the template file exists in the same "
                f"directory as this script."
            )
            return False
        except ET.ParseError as e:
            logger.error(f"Failed to parse template {template_path}: {e}")
            return False

        logger.debug(f"Loaded XML template from {template_full_path}.")

        # MongoDB fields
        meta_name = self._first_non_empty(dataframe_row, ["meta_name"])
        meta_title = self._first_non_empty(dataframe_row, ["meta_title"])
        meta_description = self._first_non_empty(dataframe_row, ["meta_description"])
        n_samples = self._first_non_empty(dataframe_row, ["system_number_of_samples"])

        upload_info = _row_get(dataframe_row, "upload_info", {}) or {}
        archive_path: str = ""
        if isinstance(upload_info, Mapping):
            archive_path = str(upload_info.get("archive_paths") or "").strip()
        else:
            archive_path = str(upload_info).strip()

        upload_uuid = self._first_non_empty(dataframe_row, ["system_upload_uuid"])
        system_uuid = self._first_non_empty(dataframe_row, ["system_uuid"])

        abstract_lines = [
            "Simulation of ASTEC Scenario:",
            f"Name: {meta_name}" if meta_name else "",
            f"Title: {meta_title}" if meta_title else "",
            f"Description: {meta_description}" if meta_description else "",
            f"Number of samples: {n_samples}" if n_samples else "",
            f"Archive path: {archive_path}" if archive_path else "",
            (
                f"upload_uuid_system_uuid: {upload_uuid}_{system_uuid}"
                if upload_uuid or system_uuid
                else ""
            ),
        ]
        abstract_text = "\n".join([x for x in abstract_lines if x])

        description_value_el = self._find_abstract_description_value(root)
        if description_value_el is None:
            logger.error("No ABSTRACT descriptionValue element found in template.")
            return False

        description_value_el.text = abstract_text

        # Also set <title> as before (prefer meta_title, fallback to meta_name)
        title_value = meta_title or meta_name
        if title_value:
            title_el: Optional[ET.Element] = None
            for node in root.iter():
                if _xml_local_name(node.tag) == "title":
                    title_el = node
                    break
            if title_el is not None:
                title_el.text = title_value
            else:
                logger.warning("No <title> element found in template.")

        # Serialize XML
        xml_data: bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)

        # Prepare upload
        url = f"{self.radar_api}datasets/{dataset_id}/metadata"
        headers = self._request_headers(
            with_auth=True, with_body=False, format_override="xml"
        )
        files = {"metadataFile": ("metadata.xml", xml_data, "application/xml")}

        logger.info(f"Uploading updated metadata XML for dataset {dataset_id} to {url}")
        logger.debug("XML to upload:\n%s", xml_data.decode("utf-8"))

        try:
            resp = self.session.post(url, headers=headers, files=files)
            logger.info(
                "POST %s -> %s ct=%s | Response: %s",
                url,
                resp.status_code,
                resp.headers.get("Content-Type"),
                resp.text[:300],
            )
            if resp.status_code == RadarReturnCode.OK:
                logger.info(
                    "Successfully updated dataset metadata (XML): %s", dataset_id
                )
                return True
            else:
                logger.error(
                    "Failed to update dataset metadata (XML): %s - %s",
                    resp.status_code,
                    resp.text,
                )
                return False
        except Exception as e:
            logger.error("Dataset metadata update error (XML): %s", e)
            return False

    def update_dataset_from_dataframe_row(
        self,
        *,
        dataframe_row: RowLike,
        dataset_id: str,
    ) -> bool:
        """Update the metadata of a dataset (XML version).

        Endpoint: POST /datasets/{datasetId}/metadata with
        multipart/form-data containing the XML file.
        """
        dataset_id_clean = _clean_id(dataset_id)
        if not dataset_id_clean:
            logger.error("Refusing to update: dataset_id is empty/NaN: %r", dataset_id)
            return False

        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return False

        url = f"{self.radar_api}datasets/{dataset_id_clean}/metadata"
        # Only set Accept, do NOT set Content-Type
        headers = self._request_headers(
            with_auth=True, with_body=False, format_override="xml"
        )
        payload = self._build_radar_dataset_payload_from_row(
            dataframe_row,
            dataset_id=dataset_id_clean,
        )
        payload = remove_empty_fields(payload)  # type: ignore[assignment]

        existing_metadata = self.get_dataset_metadata_xml(dataset_id_clean)
        logger.info(
            f"Existing metadata XML for dataset {dataset_id_clean}: "
            f"\n{existing_metadata}"
        )

        logger.info(f"Updating metadata for dataset {dataset_id_clean} at {url}")

        xml_element = _dict_to_radar_xml(payload)  # type: ignore[arg-type]
        xml_data = ET.tostring(xml_element, encoding="utf-8", xml_declaration=True)

        logger.info("XML to upload:\n%s", xml_data.decode("utf-8"))

        files = {"metadataFile": ("metadata.xml", xml_data, "application/xml")}

        try:
            resp = self.session.post(url, headers=headers, files=files)
            logger.info(
                "POST %s -> %s ct=%s | Response: %s",
                url,
                resp.status_code,
                resp.headers.get("Content-Type"),
                resp.text[:300],
            )
            if resp.status_code == RadarReturnCode.OK:
                logger.info(
                    "Successfully updated dataset metadata (XML): %s", dataset_id_clean
                )
                return True
            else:
                logger.error(
                    "Failed to update dataset metadata (XML): %s - %s",
                    resp.status_code,
                    resp.text,
                )
                return False
        except Exception as e:
            logger.error("Dataset metadata update error (XML): %s", e)
            return False

    def get_workspace_info(
        self,
    ) -> Optional[Dict[str, Any]]:
        """Get workspace information using OAuth token."""
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = f"{self.radar_api}/workspaces/{self.workspace_id}"
        headers = self._request_headers(with_auth=True, with_body=False)

        logger.info(f"Fetching workspace: {self.workspace_id}")

        try:
            response = self.session.get(url, headers=headers)

            if response.status_code == RadarReturnCode.OK:
                parsed = _best_effort_parse_body(response)
                if isinstance(parsed, dict):
                    logger.info(
                        f"Successfully retrieved workspace {self.workspace_id}."
                    )
                    logger.debug("Workspace parsed: %s.", parsed)
                    return parsed
                logger.error("Workspace response not parseable as dict.")
                logger.debug("Raw response: %s", parsed)
                return None
            else:
                logger.error(
                    f"Failed to get workspace: "
                    f"{response.status_code} - {response.text}."
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching workspace: {e}")
            return None

    def get_dataset_files(
        self,
        dataset_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get list of files in a dataset."""
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return None

        url = (
            f"{self.radar_api}/workspaces/{self.workspace_id}"
            + f"/datasets/{dataset_id}/files"
        )
        headers = self._request_headers(with_auth=True, with_body=False)

        logger.info(f"Fetching files for dataset: {dataset_id}")

        try:
            response = self.session.get(url, headers=headers)

            if response.status_code == RadarReturnCode.OK:
                parsed = _best_effort_parse_body(response)
                if isinstance(parsed, dict):
                    logger.info(
                        f"Successfully retrieved files for dataset {dataset_id}"
                    )
                    logger.debug("Files parsed: %s", parsed)
                    return parsed
                logger.error("Files response not parseable as dict.")
                logger.debug("Raw response: %s", parsed)
                return None
            else:
                logger.error(
                    f"Failed to get files: {response.status_code} - {response.text}"
                )
                return None

        except Exception as e:
            logger.error(f"Error fetching files: {e}")
            return None

    def create_datasets_from_dataframe(
        self,
        *,
        api_url: Optional[str] = None,
        only_uuids: Optional[set[str]] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
        update: bool = False,
    ) -> list[dict[str, str]]:
        """Create datasets from MongoDB using a streaming cursor."""
        results: list[dict[str, str]] = []
        created = 0
        considered = 0

        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return results

        query: Dict[str, Any] = {}
        if only_uuids:
            query["system_upload_uuid"] = {"$in": list(only_uuids)}

        projection: Dict[str, int] = {
            "_id": 0,
            "system_upload_uuid": 1,
            "system_uuid": 1,
            "system_status": 1,
            "system_user_info": 1,
            "system_number_of_samples": 1,
            "system_path": 1,
            "upload_info": 1,
            "meta_name": 1,
            "meta_title": 1,
            "meta_description": 1,
            RADAR_DATASET_ID_FIELD: 1,
        }

        coll = self.manager.database_handler.file_collection
        cursor = coll.find(query, projection=projection).batch_size(200)

        try:
            for row in cursor:
                upload_uuid = _clean_id(row.get("system_upload_uuid", "")) or ""

                if "system_status" in row:
                    try:
                        status = AssasDocumentFileStatus(row.get("system_status"))
                        if status != AssasDocumentFileStatus.VALID:
                            logger.info(
                                "Skipping upload_uuid=%s due to system_status=%s",
                                upload_uuid,
                                status,
                            )
                            continue
                    except Exception:
                        pass

                considered += 1
                if limit is not None and considered > int(limit):
                    logger.info("Reached limit=%s (after filtering); stopping.", limit)
                    break

                system_uuid = _clean_id(row.get("system_uuid"))
                existing_dataset_id = _clean_id(row.get(RADAR_DATASET_ID_FIELD))

                if not existing_dataset_id and system_uuid:
                    try:
                        document: Optional[dict[str, Any]] = (
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
                            "Could not fetch existing document for system_uuid=%s "
                            "to check %s: %s",
                            system_uuid,
                            RADAR_DATASET_ID_FIELD,
                            e,
                        )

                if existing_dataset_id:
                    if update:
                        if dry_run:
                            logger.info(
                                "[DRY-RUN] Would update metadata for "
                                "upload_uuid=%s dataset_id=%s.",
                                upload_uuid,
                                existing_dataset_id,
                            )
                            results.append(
                                {
                                    "system_upload_uuid": upload_uuid,
                                    "dataset_id": existing_dataset_id,
                                }
                            )
                            continue

                        ok = self.update_radar_metadata_with_template(
                            dataframe_row=row,
                            dataset_id=existing_dataset_id,
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
                                "Failed updating dataset metadata for "
                                "upload_uuid=%s dataset_id=%s",
                                upload_uuid,
                                existing_dataset_id,
                            )
                        continue

                    logger.info(
                        "Skipping creation for upload_uuid=%s because %s already "
                        "exists: %s.",
                        upload_uuid,
                        RADAR_DATASET_ID_FIELD,
                        existing_dataset_id,
                    )
                    results.append(
                        {
                            "system_upload_uuid": upload_uuid,
                            "dataset_id": existing_dataset_id,
                        }
                    )
                    continue

                if dry_run:
                    logger.info(
                        "[DRY-RUN] Would create dataset for upload_uuid=%s.",
                        upload_uuid,
                    )
                    results.append(
                        {"system_upload_uuid": upload_uuid, "dataset_id": ""}
                    )
                    created += 1
                    continue

                dataset_id = self.create_dataset_from_dataframe_row(
                    row, api_url=api_url
                )
                if not dataset_id:
                    logger.error(
                        "Failed creating dataset for upload_uuid=%s",
                        upload_uuid,
                    )
                    continue

                if system_uuid:
                    self.persist_radar_dataset_id(
                        upload_uuid=upload_uuid,
                        system_uuid=system_uuid,
                        dataset_id=dataset_id,
                    )
                else:
                    logger.error(
                        "Created dataset_id=%s for upload_uuid=%s but cannot persist: "
                        "missing system_uuid in row.",
                        dataset_id,
                        upload_uuid,
                    )

                results.append(
                    {
                        "system_upload_uuid": upload_uuid,
                        "dataset_id": dataset_id,
                    }
                )
                logger.info(
                    "Created dataset_id=%s for upload_uuid=%s.",
                    dataset_id,
                    upload_uuid,
                )
                created += 1
        finally:
            cursor.close()

        return results

    def get_workspace_info_old(
        self,
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

        url = f"{self.radar_api}/workspaces/{self.workspace_id}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        logger.info(f"Fetching workspace: {self.workspace_id}")

        try:
            response = self.session.get(url, headers=headers)

            if response.status_code == RadarReturnCode.OK:
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

    def access_assas_workspace(self) -> None:
        """Access ASSAS workspace."""
        logger.info("Accessing workspace info...")
        workspace_info = self.get_workspace_info()

        if workspace_info:
            self.plot_workspace_properties(workspace_info)
        else:
            logger.error("Could not access workspace")

    def get_dataset_ids_from_workspace(
        self,
        *,
        limit: Optional[int] = None,
        force_json: bool = True,
    ) -> list[str]:
        """Fetch dataset IDs for the configured workspace using the /children endpoint.

        Returns a list of dataset IDs.
        """
        if not self.access_token:
            if not self.get_access_token():
                logger.error("Could not obtain access token")
                return []

        ws_url = f"{self.radar_api}workspaces/{self.workspace_id}/children"
        fmt: RadarFormat = "json" if force_json else self.radar_format
        headers = self._request_headers(
            with_auth=True, with_body=False, format_override=fmt
        )

        try:
            resp = self.session.get(ws_url, headers=headers)
            logger.info(
                "GET %s -> %s ct=%s | Response: %s",
                ws_url,
                resp.status_code,
                resp.headers.get("Content-Type"),
                resp.text[:300],
            )

            if resp.status_code == RadarReturnCode.OK:
                parsed = _best_effort_parse_body(resp)
                # Expecting: {"data": [ { "id": ... }, ... ]}
                if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
                    ids = [
                        cleaned_id
                        for entry in parsed["data"]
                        if isinstance(entry, dict)
                        and (cleaned_id := _clean_id(entry.get("id")))
                    ]
                    if limit is not None:
                        ids = ids[: int(limit)]
                    return ids
                else:
                    logger.warning("Unexpected response structure: %r", parsed)
                    return []
            else:
                logger.error(
                    "Workspace GET failed (%s). body=%r",
                    resp.status_code,
                    (resp.text or "")[:300],
                )
                return []
        except Exception as e:
            logger.error("Workspace GET error: %s", e)
            return []


def _verify_file_collection_access(database_manager: AssasDatabaseManager) -> None:
    """Fail fast if Atlas user cannot read file_collection."""
    try:
        coll = database_manager.database_handler.file_collection
        _ = coll.find_one({}, {"_id": 1})  # read test
        logger.info("MongoDB access check OK: file_collection is readable.")
    except Exception as e:
        logger.error("MongoDB access check failed for file_collection: %s", e)
        raise PermissionError(
            "Cannot read MongoDB file_collection. "
            "Check Atlas Database Access role (readWrite), Network Access allowlist, "
            "and CONNECTIONSTRING credentials."
        ) from e


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
            "datasets for these system_upload_uuid(s)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "When used with --action from-db: "
            "create only the first N datasets (after filtering)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "When used with --action from-db: "
            "do not call RADAR, only log what would happen."
        ),
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help=(
            "When used with --action from-db: if radar_dataset_id already exists, "
            "update that dataset's metadata instead of creating a new one."
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
    parser.add_argument(
        "--radar-format",
        type=str,
        default="xml",
        choices=["xml", "json"],
        help=(
            "RADAR API payload/accept format (default: xml). "
            "Use json to keep old behavior."
        ),
    )
    parser.add_argument(
        "--get-dataset-ids",
        action="store_true",
        default=False,
        help="Fetch and print dataset IDs from the configured RADAR workspace.",
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

    _verify_file_collection_access(database_manager)

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
        radar_format=args.radar_format,
    )

    if args.action in ["workspace", "both"]:
        oauth_client.access_assas_workspace()

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

    if args.get_dataset_ids:
        ds_ids = oauth_client.get_dataset_ids_from_workspace()
        logger.info(
            "Dataset IDs in workspace %s: %s", oauth_client.workspace_id, ds_ids
        )
