"""Generate DataCite JSON metadata files for datasets in the ASSAS database."""

import os
import json
import logging

from pathlib import Path
from pandas import Series
from datetime import date

from assasdb import (
    AssasDatabaseManager,
    AssasDatabaseHandler,
    AssasDocumentFileStatus,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BACKUP_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/backup_mongodb"
DATACITE_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/datacite_json"


def generate_datacite_json(entry: Series, output_dir: str) -> str:
    """Generate a DataCite JSON metadata file for a given database entry."""
    file_path = entry["system_result"]
    dataset_title = f"ASSAS Simulation Dataset: \
            {entry.get('meta_name', os.path.basename(file_path))}"
    creators = [
        {
            "name": "Jonas Dressner",
            "affiliation": ["Karlsruhe Institute of Technology"],
        }
    ]
    if entry.get("system_user"):
        creators.append(
            {
                "name": entry["system_user"],
                "affiliation": ["Karlsruhe Institute of Technology"],
            }
        )

    publisher = "ASSAS Data Hub / KIT"
    publication_year = str(entry.get("system_date", date.today().year))[:4]
    resource_type = "Dataset"
    description = entry.get(
        "meta_description",
        "This dataset contains ASTEC simulation outputs for training "
        + "surrogate models in nuclear reactor severe accident scenarios.",
    )
    rights = {
        "rights": "CC-BY 4.0",
        "rightsURI": "https://creativecommons.org/licenses/by/4.0/",
    }
    subjects = [
        {"subject": "Nuclear reactor", "subjectScheme": "Keyword"},
        {"subject": "Severe accident", "subjectScheme": "Keyword"},
        {"subject": "Simulation dataset", "subjectScheme": "Keyword"},
    ]
    related_identifiers = [
        {
            "relatedIdentifier": entry.get(
                "meta_repo_url", "https://github.com/ASSAS-project/repo"
            ),
            "relatedIdentifierType": "URL",
            "relationType": "IsSupplementTo",
        }
    ]
    doi = entry.get(
        "meta_doi", f"10.35097/assas.{entry.get('system_name', 'unknown')}.v1"
    )

    datacite_json = {
        "data": {
            "id": f"doi:{doi}",
            "type": "dois",
            "attributes": {
                "doi": doi,
                "event": "publish",
                "creators": creators,
                "titles": [{"title": dataset_title}],
                "publisher": publisher,
                "publicationYear": publication_year,
                "types": {"resourceTypeGeneral": resource_type},
                "descriptions": [
                    {"description": description, "descriptionType": "Abstract"}
                ],
                "rightsList": [rights],
                "subjects": subjects,
                "relatedIdentifiers": related_identifiers,
            },
        }
    }

    out_name = f"{entry.get('system_uuid', 'unknown')}_datacite.json"
    out_path = os.path.join(output_dir, out_name)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(datacite_json, f, indent=2)

    return out_path


if __name__ == "__main__":
    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            client=None, backup_directory=BACKUP_DIRECTORY
        ),
    )
    dataframe = database_manager.get_all_database_entries_from_backup()

    datacite_dir = Path(DATACITE_DIRECTORY)
    datacite_dir.mkdir(parents=True, exist_ok=True)

    for idx, entry in dataframe.iterrows():
        try:
            if entry["system_status"] != AssasDocumentFileStatus.VALID.value:
                continue
            out_path = generate_datacite_json(entry, datacite_dir)
            logger.info(f"Generated DataCite JSON: {out_path}")
        except Exception as e:
            logger.error(f"Error for entry {entry.get('system_name', idx)}: {e}")
