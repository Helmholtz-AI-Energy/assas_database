"""Scan LSDF upload_test folders for upload_info.pickle.

The script searchs root then one level deeper.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
import shutil  # added for copying files
import pickle
import subprocess

from assasdb import AssasDatabaseHandler, AssasDatabaseManager

# logging setup
log_file_path = Path(__file__).parent / "find_upload_info.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(log_file_path), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

LSDF_DATA_DIR = "ASSAS/upload_test"
TARGET_FILENAME = "upload_info.pickle"
LIMIT_SAMPLES = 80000
BACKUP_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/backup_mongodb"


def get_database_entries() -> pd.DataFrame:
    """Return all database entries from the backup directory.

    This function initializes an instance of `AssasDatabaseManager` with the
    specified backup directory and retrieves all database entries.
    """
    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            client=None, backup_directory=BACKUP_DIRECTORY
        ),
    )

    print(f"Get all database entries from backup directory: {BACKUP_DIRECTORY}.")
    database_entries = database_manager.get_all_database_entries_from_backup()
    print(f"Number of database entries: {len(database_entries)}.")

    return database_entries


def find_upload_info_in_project(project_dir: Path) -> Optional[Path]:
    """Check for TARGET_FILENAME in project_dir.

    If not found, check immediate subdirectories (one level deeper).
    Return the Path to the found file or None.
    """
    if not project_dir.exists() or not project_dir.is_dir():
        return None

    candidate = project_dir / TARGET_FILENAME
    if candidate.exists():
        return candidate

    # search one directory deeper
    for child in project_dir.iterdir():
        if child.is_dir():
            candidate = child / TARGET_FILENAME
            if candidate.exists():
                return candidate

    return None


def correct_archive_paths_for_user(lsdf_projects_root: Path, user: str) -> None:
    """Correct archive paths for a specific user."""
    lsdf_projects_root = Path(lsdf_projects_root)
    lsdf_data_dir = lsdf_projects_root / LSDF_DATA_DIR

    dataframe = get_database_entries()
    # Implementation would go here
    number_of_corrected_archives = 0
    for index, row in dataframe.iterrows():
        logger.info(
            f"Processing row {index} for user "
            f"{row['system_user']} and archive {row['meta_name']}"
        )
        # Example: Correcting a specific field
        if row["system_user"] == user:
            upload_info_path = (
                lsdf_data_dir / row["system_upload_uuid"] / TARGET_FILENAME
            )
            logger.info(f"Correcting upload info for user {user} at {upload_info_path}")
            if upload_info_path:
                old_upload_info = get_upload_info(upload_info_path)
                archive_name = old_upload_info["name"]
                old_archive_paths = old_upload_info["archive_paths"]
                new_archive_paths = [
                    f"./{archive_name}/LOCA_6I_CL_1300_LIKE_SIMPLIFIED_ASSAS_FILT.bin"
                ]
                logger.info(
                    f"Updating archive_path from {old_archive_paths} "
                    f"to {new_archive_paths}"
                )
                correct_archive_paths_in_upload_info(
                    upload_info_path, "archive_paths", new_archive_paths
                )
                generate_reload_file(
                    lsdf_data_dir / row["system_upload_uuid"], row["system_upload_uuid"]
                )
                number_of_corrected_archives += 1


def generate_reload_file(archive_path: Path, upload_uuid: str) -> None:
    """Trigger a reload of the specified archive."""
    logger.info(f"Triggering reload for archive: {archive_path}")
    # Implementation for reloading the archive would go here
    try:
        reload_file = archive_path / f"{upload_uuid}_reload"
        subprocess.run(["touch", str(reload_file)], check=True)
        logger.info(f"Generate reload file via system command: {reload_file}")
    except Exception as e:
        logger.error(f"Failed to trigger reload for {upload_uuid}: {e}")


def get_upload_info(upload_info_path: Path) -> Optional[dict]:
    """Retrieve upload information from the specified upload_info.pickle file."""
    if not upload_info_path.exists():
        logger.error(f"Upload info file does not exist: {upload_info_path}")
        return None

    try:
        with open(upload_info_path, "rb") as f:
            upload_info = pickle.load(f)
            logger.info(f"Loaded upload info from {upload_info_path}")
            return upload_info
    except Exception as e:
        logger.error(f"Failed to load upload info from {upload_info_path}: {e}")
        return None


def correct_archive_paths_in_upload_info(
    upload_info_path: Path, key: str, value: list[str]
) -> None:
    """Correct the upload_info.pickle file if necessary."""
    if not upload_info_path.exists():
        logger.error(f"Upload info file does not exist: {upload_info_path}")
        return

    # Load the existing upload info
    try:
        with open(upload_info_path, "rb") as f:
            upload_info = pickle.load(f)
    except Exception as e:
        logger.error(f"Failed to load upload info from {upload_info_path}: {e}")
        return

    # Perform corrections (this is just a placeholder)
    # In a real implementation, you would modify the upload_info as needed
    upload_info[key] = value

    # Save the corrected upload info
    try:
        with open(upload_info_path, "wb") as f:
            pickle.dump(upload_info, f)
    except Exception as e:
        logger.error(f"Failed to save corrected upload info to {upload_info_path}: {e}")


def scan_lsdf_uploads(lsdf_projects_root: Path) -> list[dict]:
    """Scan all directories under lsdf_projects_root/LSDF_DATA_DIR.

    Returns a list of dicts with keys:
    project_dir, found, found_path, search_depth, copied_to_top
    """
    base = lsdf_projects_root / LSDF_DATA_DIR
    results = []

    if not base.exists():
        logger.error(f"Base directory does not exist: {base}")
        return results

    logger.info(f"Scanning upload_test base: {base}")

    for entry in sorted(base.iterdir()):
        if not entry.is_dir():
            continue

        found_path = find_upload_info_in_project(entry)
        if found_path is None:
            results.append(
                {
                    "project_dir": str(entry),
                    "found": False,
                    "found_path": None,
                    "search_depth": None,
                    "copied_to_top": False,
                }
            )
            logger.info(f"Not found: {entry}")
        else:
            # determine depth: 0 if in project dir, 1 if in a subdirectory
            found_path = Path(found_path)
            depth = 0 if (found_path.parent == entry) else 1

            copied = False
            if depth == 1:
                top_target = entry / TARGET_FILENAME
                try:
                    # copy (overwrite) from subdirectory to top-level project dir
                    shutil.copy2(found_path, top_target)
                    copied = True
                    logger.info(f"Copied {found_path} to {top_target}")
                except Exception as e:
                    logger.error(f"Failed to copy {found_path} to {top_target}: {e}")
                    copied = False

            results.append(
                {
                    "project_dir": str(entry),
                    "found": True,
                    "found_path": str(found_path),
                    "search_depth": depth,
                    "copied_to_top": copied,
                }
            )
            logger.info(
                f"Found ({depth}) {found_path} for project {entry} "
                f"(copied_to_top={copied})"
            )

    return results


if __name__ == "__main__":
    lsdf_projects = os.environ.get("LSDFPROJECTS")
    if not lsdf_projects:
        logger.error("Environment variable LSDFPROJECTS is not set.")
        sys.exit(1)

    confirmation = input(
        "DANGER: This script can modify LSDF data. Type 'I UNDERSTAND' to proceed: "
    )

    if confirmation.strip() != "I UNDERSTAND":
        logger.error("Confirmation not provided. Aborting.")
        sys.exit(1)

    lsdf_root = Path(lsdf_projects)
    scanned = scan_lsdf_uploads(lsdf_root)
    correct_archive_paths_for_user(lsdf_root, user="sw5174@partner.kit.edu")

    # Save results
    out_csv = Path(__file__).parent / "upload_info_scan_results.csv"
    df = pd.DataFrame(scanned)
    df.to_csv(out_csv, index=False)
    logger.info(f"Scan complete. Results written to {out_csv}")
