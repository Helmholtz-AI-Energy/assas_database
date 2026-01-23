"""Assas Database Validator CLI.

Scans the LSDF upload directory for upload_uuid folders (folder names that are valid
UUIDs), queries the target database for registered upload_uuid values, and produces
a consistency report listing:
 - upload UUIDs present on LSDF but not registered in DB (lsdf_only)
 - upload UUIDs registered in DB but missing on LSDF (db_only)
 - counts of DB documents per upload_uuid (db_counts)

Additional validations added:
 - Check for trigger file: a file with the upload_uuid as filename at the top level
   of the upload folder.
 - Check for upload_info.pickle inside the upload folder; if present and the DB has
   no entry for that upload_uuid, load the pickle and insert it as a new document in
   the files collection.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Set, List, Tuple

import uuid
import pickle

from pymongo import MongoClient
from pymongo.collection import Collection

from assasdb.assas_database_manager import AssasDatabaseManager
from assasdb.assas_database_handler import AssasDatabaseHandler

logger = logging.getLogger("assas_database_validator")


def setup_logging(level: int = logging.INFO) -> None:
    """Set up the logging configuration."""
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=level, format=fmt)


def scan_lsdf_upload_uuids(upload_dir: Path) -> Set[str]:
    """Return set of folder names in upload_dir that are valid UUIDs."""
    upload_uuids: Set[str] = set()
    if not upload_dir.exists():
        logger.error("Upload directory does not exist: %s", upload_dir)
        return upload_uuids

    for entry in upload_dir.iterdir():
        if not entry.is_dir():
            continue
        name = entry.name
        try:
            uuid.UUID(name)
            upload_uuids.add(name)
        except ValueError:
            logger.debug("Skipping non-UUID folder: %s", name)
            continue

    logger.info("Found %d upload UUID folders on LSDF", len(upload_uuids))
    return upload_uuids


def query_db_upload_uuids(manager: AssasDatabaseManager) -> Dict[str, int]:
    """Query DB and return a mapping upload_uuid.

    The Number of documents in DB for that upload_uuid.
    This tries to use database_handler.get_all_file_documents()
    if available; otherwise falls back to manager.get_all_database_entries() DataFrame.
    """
    counts: Dict[str, int] = {}
    handler = manager.database_handler

    # Preferred: handler provides a list of documents
    try:
        docs = handler.get_all_file_documents()
        if docs is None:
            docs = []
    except Exception:
        docs = None

    if docs is None:
        # fallback to DataFrame based accessor
        try:
            df = manager.get_all_database_entries()
            if df is None or df.empty:
                logger.info("No documents found in DB (DataFrame empty).")
                return {}
            if "system_upload_uuid" not in df.columns:
                logger.warning("system_upload_uuid not present in DB DataFrame.")
                return {}
            for val in df["system_upload_uuid"].tolist():
                if val is None:
                    continue
                key = str(val)
                counts[key] = counts.get(key, 0) + 1
            logger.info(
                "Collected %d upload UUIDs from DB (DataFrame fallback).", len(counts)
            )
            return counts
        except Exception as e:
            logger.error("Failed to read DB via DataFrame fallback: %s", e)
            return {}

    # Build counts from docs list
    for doc in docs:
        val = doc.get("system_upload_uuid")
        if not val:
            continue
        key = str(val)
        counts[key] = counts.get(key, 0) + 1

    logger.info("Collected %d upload UUIDs from DB documents.", len(counts))
    return counts


def is_valid_archive_folder(folder: Path, upload_uuid: str) -> Tuple[bool, List[str]]:
    """Check validity of an upload folder.

    Validity criteria:
      - Accept trigger files at top-level named:
          * {upload_uuid}
          * {upload_uuid}_converting
          * {upload_uuid}_valid
      - upload_info.pickle exists at top-level

    Returns (is_valid, list_of_issues)
    """
    issues: List[str] = []
    trigger_primary = folder / upload_uuid
    trigger_converting = folder / f"{upload_uuid}_converting"
    trigger_valid = folder / f"{upload_uuid}_valid"

    # Accept any of the three trigger variants
    if not (
        (trigger_primary.exists() and trigger_primary.is_file())
        or (trigger_converting.exists() and trigger_converting.is_file())
        or (trigger_valid.exists() and trigger_valid.is_file())
    ):
        issues.append("missing_trigger_file")

    upload_info = folder / "upload_info.pickle"
    if not upload_info.exists() or not upload_info.is_file():
        issues.append("missing_upload_info")

    is_valid = len(issues) == 0
    return is_valid, issues


def load_upload_info_pickle(pickle_path: Path) -> dict | None:
    """Load upload_info.pickle and return dict.

    Args:
        pickle_path: Path to upload_info.pickle file.

    Returns:
        None on error.

    """
    try:
        with pickle_path.open("rb") as fh:
            data = pickle.load(fh)
        if not isinstance(data, dict):
            logger.warning("upload_info.pickle content is not a dict: %s", pickle_path)
            return None
        return data
    except Exception as e:
        logger.error("Failed to load pickle %s: %s", pickle_path, e)
        return None


def ensure_db_has_upload_info(
    collection: Collection, upload_uuid: str, info_doc: dict
) -> Tuple[bool, str]:
    """Ensure the DB collection has a document for system_upload_uuid.

    If a document exists but lacks 'upload_info', add it from info_doc.
    If no document exists, do NOT insert a new one (just report).

    Args:
      collection: pymongo Collection object
      upload_uuid: the upload UUID to check
      info_doc: dict to insert as 'upload_info' if needed

    Returns (success, message):
      - success: bool indicating if operation was successful
      - message: one of:
          * "upload_info_present" - document exists and has upload_info
          * "updated" - document existed and was updated with upload_info
          * "update_noop" - document existed but update did not modify
          * "no_upload_uuid" - no document exists for this upload_uuid
          * "error:{error_message}" - error occurred during operation

    """
    try:
        existing = collection.find_one({"system_upload_uuid": upload_uuid})
        if existing:
            # If upload_info already present, leave untouched
            if "upload_info" in existing:
                return True, "upload_info_present"

            # Add upload_info to the existing document
            update_result = collection.update_one(
                {"_id": existing["_id"]},
                {
                    "$set": {
                        "upload_info": info_doc,
                        "system_imported_from": "upload_info.pickle",
                    }
                },
            )
            if update_result.modified_count > 0:
                logger.info(
                    "Updated existing DB document for upload_uuid %s with upload_info",
                    upload_uuid,
                )
                return True, "updated"
            else:
                logger.info(
                    "No update performed for upload_uuid %s (update_result=%s)",
                    upload_uuid,
                    update_result.raw_result,
                )
                return False, "update_noop"

        logger.warning("No existing DB document for upload_uuid %s", upload_uuid)
        return False, "no_upload_uuid"

    except Exception as e:
        logger.error(
            "Failed to ensure upload_info for %s: %s", upload_uuid, e, exc_info=True
        )
        return False, f"error:{e}"


def validate_lsdf_vs_db(
    upload_dir: Path,
    manager: AssasDatabaseManager,
    mongo_collection: Collection | None = None,
) -> Dict:
    """Compare LSDF folders and DB registrations.

    Args:
        upload_dir: Path to LSDF upload directory
        manager: AssasDatabaseManager instance
        mongo_collection: Optional pymongo Collection for direct DB access

    Returns:
        report dict with validation results and details.

    """
    lsdf_set = scan_lsdf_upload_uuids(upload_dir)
    db_counts = query_db_upload_uuids(manager)
    db_set = set(db_counts.keys())

    lsdf_only = sorted(list(lsdf_set - db_set))
    db_only = sorted(list(db_set - lsdf_set))

    # extra validation details
    invalid_archives: List[str] = []
    missing_pickles: List[str] = []
    already_present: List[str] = []
    needs_update: List[str] = []  # present in DB but missing upload_info
    missing_in_db: List[str] = []  # valid archive but no DB document present
    check_errors: Dict[str, str] = {}

    total = len(lsdf_set)
    logger.info("Starting LSDF <> DB validation: %d LSDF folders to check", total)

    collection = None
    if mongo_collection is not None:
        collection = mongo_collection
    else:
        try:
            # try to get pymongo collection from handler if available
            handler = manager.database_handler
            collection = getattr(handler, "files_collection", None)
            if collection is None:
                # fallback: attempt to build client from handler attributes
                client = getattr(handler, "client", None)
                dbname = getattr(handler, "database_name", None)
                if client and dbname:
                    collection = client[dbname]["files"]
        except Exception:
            collection = None

    # how often to emit progress (tuneable)
    log_every = 50 if total > 200 else 10

    for idx, upload_uuid in enumerate(sorted(lsdf_set), start=1):
        # periodic progress log
        if idx % log_every == 0 or idx == 1 or idx == total:
            pct = (idx / total * 100) if total > 0 else 100.0
            logger.info(
                "Progress: %d/%d (%.1f%%) - checking upload_uuid=%s",
                idx,
                total,
                pct,
                upload_uuid,
            )

        folder = upload_dir / upload_uuid
        valid, issues = is_valid_archive_folder(folder, upload_uuid)
        if not valid:
            logger.debug("Archive %s invalid: %s", upload_uuid, issues)
            if "missing_trigger_file" in issues:
                invalid_archives.append(upload_uuid)
            if "missing_upload_info" in issues:
                missing_pickles.append(upload_uuid)
            # skip further checks for invalid archive
            continue

        # valid archive: load pickle (we only read it for reporting; no DB insert)
        pickle_path = folder / "upload_info.pickle"
        info = load_upload_info_pickle(pickle_path)
        if info is None:
            logger.warning("Failed to load upload_info.pickle for %s", upload_uuid)
            missing_pickles.append(upload_uuid)
            continue
        else:
            logger.debug(
                "Loaded upload_info.pickle for %s (keys=%s)",
                upload_uuid,
                list(info.keys())[:10],
            )

        # If collection available, check DB status without modifying
        if collection is not None:
            ok, msg = ensure_db_has_upload_info(collection, upload_uuid, info)
            if msg == "upload_info_present":
                already_present.append(upload_uuid)
                logger.debug("DB already has upload_info for %s", upload_uuid)
            elif msg == "present_missing_upload_info":
                needs_update.append(upload_uuid)
                logger.info(
                    "DB has document for %s but missing upload_info", upload_uuid
                )
            elif msg == "missing_in_db" or msg == "no_upload_uuid":
                missing_in_db.append(upload_uuid)
                logger.info("No DB document found for %s", upload_uuid)
            else:
                check_errors[upload_uuid] = msg
                logger.error("Error checking DB for %s: %s", upload_uuid, msg)
        else:
            logger.debug(
                "No DB collection available to check upload_info for %s", upload_uuid
            )

    # final summary logs
    logger.info(
        "Validation finished: LSDF=%d, DB=%d, lsdf_only=%d, "
        "db_only=%d, invalid_archives=%d, missing_pickles=%d",
        total,
        len(db_set),
        len(lsdf_only),
        len(db_only),
        len(invalid_archives),
        len(missing_pickles),
    )
    logger.info(
        "Details: already_present=%d, needs_update=%d, missing_in_db=%d, errors=%d",
        len(already_present),
        len(needs_update),
        len(missing_in_db),
        len(check_errors),
    )

    report = {
        "lsdf_count": len(lsdf_set),
        "db_count": len(db_set),
        "lsdf_only": lsdf_only,
        "db_only": db_only,
        "db_counts": db_counts,
        "invalid_archives": invalid_archives,
        "missing_pickles": missing_pickles,
        "already_present": already_present,
        "needs_update": needs_update,
        "missing_in_db": missing_in_db,
        "check_errors": check_errors,
    }

    return report


def write_report(report: Dict, outfile: Path) -> None:
    """Write report dict as JSON to outfile."""
    try:
        outfile.parent.mkdir(parents=True, exist_ok=True)
        with outfile.open("w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        logger.info("Wrote validation report to %s", outfile)
    except Exception as e:
        logger.error("Failed to write report %s: %s", outfile, e)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    p = argparse.ArgumentParser(description="ASSAS DB <> LSDF Validator")
    p.add_argument(
        "--upload-dir",
        "-u",
        default="/mnt/ASSAS/upload_test",
        help="LSDF upload directory",
    )
    p.add_argument(
        "--conn",
        "-c",
        default="mongodb://localhost:27017/",
        help="MongoDB connection string (default: local)",
    )
    p.add_argument(
        "--db", "-d", default="assas_dev", help="Database name (default: assas_dev)"
    )
    p.add_argument(
        "--report", "-r", default=None, help="Write JSON report to this path"
    )
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    p.add_argument(
        "--delete-missing",
        action="store_true",
        help="Delete DB documents for upload UUIDs missing on LSDF (destructive)",
    )
    p.add_argument(
        "--delete-duplicates",
        action="store_true",
        help=(
            "Delete duplicate DB documents for upload UUIDs "
            "with multiple entries (destructive)"
        ),
    )
    p.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help=(
            "Auto confirm destructive actions "
            "(use with --delete-missing / --delete-duplicates)",
        ),
    )
    return p.parse_args()


def main() -> int:
    """Execute main CLI logic."""
    args = parse_args()
    setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    # Build database handler / manager
    handler_kwargs = {}
    if args.conn:
        handler_kwargs["connection_string"] = args.conn
    if args.db:
        handler_kwargs["database_name"] = args.db

    try:
        db_handler = AssasDatabaseHandler(database_name=args.db)
    except Exception as e:
        logger.error("Failed to create AssasDatabaseHandler: %s", e)
        return 2

    manager = AssasDatabaseManager(
        database_handler=db_handler, upload_directory=args.upload_dir
    )
    upload_dir = Path(args.upload_dir)

    # create a direct pymongo collection handle for insertion / deletion if possible
    mongo_collection = None
    try:
        client = getattr(db_handler, "client", None)
        dbname = getattr(db_handler, "database_name", args.db)
        if client is None:
            client = MongoClient(args.conn)
        mongo_collection = client[dbname]["files"]
    except Exception as e:
        logger.warning("Could not obtain direct pymongo collection: %s", e)
        mongo_collection = None

    logger.info("Beginning validation run (upload_dir=%s, db=%s)", upload_dir, args.db)
    report = validate_lsdf_vs_db(
        upload_dir=upload_dir, manager=manager, mongo_collection=mongo_collection
    )
    logger.info("Validation run completed")

    # Print human readable summary
    logger.info("\nASSAS LSDF <> DB Validation Report")
    logger.info("=================================")
    logger.info(f"LSDF upload folders : {report['lsdf_count']}")
    logger.info(f"DB registered UUIDs : {report['db_count']}")
    logger.info(f"Only on LSDF        : {len(report['lsdf_only'])}")
    logger.info(f"Only in DB          : {len(report['db_only'])}")
    logger.info(f"Invalid archives    : {len(report['invalid_archives'])}")
    logger.info(f"Missing pickles     : {len(report['missing_pickles'])}")

    # safe access for optional keys
    inserted_docs = report.get("inserted_docs", [])
    logger.info(f"Inserted from pickle: {len(inserted_docs)}")
    logger.info(f"Already present     : {len(report.get('already_present', []))}")
    if report.get("lsdf_only"):
        logger.info("\nUpload UUIDs present on LSDF but NOT registered in DB:")
        for u in report["lsdf_only"]:
            logger.info(f"  - {u}")

    if report.get("db_only"):
        logger.info("\nUpload UUIDs registered in DB but MISSING on LSDF:")
        for u in report["db_only"]:
            cnt = report["db_counts"].get(u, 0)
            logger.info(f"  - {u} (documents in DB: {cnt})")

    if report.get("invalid_archives"):
        logger.info("\nInvalid archives (missing trigger file):")
        for u in report["invalid_archives"]:
            logger.info(f"  - {u}")

    if report.get("missing_pickles"):
        logger.info("\nUpload folders missing upload_info.pickle:")
        for u in report["missing_pickles"]:
            logger.info(f"  - {u}")

    # insertion / check errors (report may use 'insertion_errors' or 'check_errors')
    insertion_errors = report.get("insertion_errors", report.get("check_errors", {}))
    if insertion_errors:
        logger.info("\nInsertion / check errors:")
        for u, msg in insertion_errors.items():
            logger.info(f"  - {u}: {msg}")

    # --- Existing: delete DB documents for db_only upload UUIDs (destructive) ---
    if args.delete_missing:
        if mongo_collection is None:
            logger.error(
                "Deletion requested but no MongoDB collection available. "
                "Aborting deletion."
            )
            logger.error("Cannot perform deletion - no MongoDB collection available.")
        elif not report.get("db_only"):
            logger.info("No DB-only upload UUIDs found. Nothing to delete.")
        else:
            uuids_to_delete = report["db_only"]
            logger.info(
                f"About to DELETE documents for {len(uuids_to_delete)} "
                f"upload_uuid(s) from DB."
            )
            if not args.yes:
                confirm = input(
                    "Type 'DELETE' to confirm destructive deletion: "
                ).strip()
                if confirm != "DELETE":
                    logger.info("Deletion aborted by user.")
                    logger.info("User aborted deletion of DB-only upload UUIDs.")
                    proceed_delete = False
                else:
                    proceed_delete = True
            else:
                proceed_delete = True

            if proceed_delete:
                try:
                    # delete all documents that reference any of the missing
                    # upload_uuid values
                    res = mongo_collection.delete_many(
                        {"system_upload_uuid": {"$in": uuids_to_delete}}
                    )
                    logger.info(
                        "Deleted %d document(s) from collection '%s'.",
                        res.deleted_count,
                        mongo_collection.name,
                    )
                    logger.info(
                        "Deleted %d documents for upload_uuids: %s",
                        res.deleted_count,
                        uuids_to_delete,
                    )
                    # update report with deletion summary
                    report["deleted_count"] = int(res.deleted_count)
                    report["deleted_upload_uuids"] = uuids_to_delete
                except Exception as e:
                    logger.error(
                        "Failed to delete documents for upload_uuids %s: %s",
                        uuids_to_delete,
                        e,
                        exc_info=True,
                    )
                    logger.error(f"ERROR: Deletion failed: {e}")

    # --- New: detect duplicated upload_uuids and optionally delete extra documents ---
    duplicate_uuids = [u for u, c in report.get("db_counts", {}).items() if c > 1]
    if duplicate_uuids:
        logger.info(
            f"Found {len(duplicate_uuids)} upload_uuid(s) with "
            "multiple DB entries (duplicates)."
        )
        for u in duplicate_uuids:
            logger.info(f"  - {u} (count={report['db_counts'].get(u)})")

    if args.delete_duplicates:
        if mongo_collection is None:
            logger.error(
                "Duplicate deletion requested but no MongoDB collection available. "
                "Aborting."
            )
            logger.error(
                "Cannot perform duplicate deletion - no MongoDB collection available."
            )
        elif not duplicate_uuids:
            logger.info("No duplicate upload UUIDs found. Nothing to delete.")
        else:
            logger.info(
                f"About to REMOVE duplicate documents for "
                f"{len(duplicate_uuids)} upload_uuid(s)."
            )
            if not args.yes:
                confirm = input(
                    "Type 'DELETE_DUPLICATES' to confirm "
                    "destructive duplicate removal: "
                ).strip()
                if confirm != "DELETE_DUPLICATES":
                    logger.info("Duplicate deletion aborted by user.")
                    logger.info("User aborted duplicate deletion.")
                    proceed_dup_delete = False
                else:
                    proceed_dup_delete = True
            else:
                proceed_dup_delete = True

            if proceed_dup_delete:
                total_deleted = 0
                per_uuid_deleted: Dict[str, int] = {}
                for u in duplicate_uuids:
                    try:
                        # fetch all doc _id for this upload_uuid, keep one,
                        # delete the rest
                        docs = list(
                            mongo_collection.find(
                                {"system_upload_uuid": u}, projection=["_id"]
                            )
                        )
                        if len(docs) <= 1:
                            per_uuid_deleted[u] = 0
                            continue
                        ids = [d["_id"] for d in docs]
                        ids_to_delete = ids[1:]
                        res = mongo_collection.delete_many(
                            {"_id": {"$in": ids_to_delete}}
                        )
                        per_uuid_deleted[u] = int(res.deleted_count)
                        total_deleted += int(res.deleted_count)
                        logger.info(
                            "Deleted %d duplicate document(s) for upload_uuid %s",
                            res.deleted_count,
                            u,
                        )
                    except Exception as e:
                        per_uuid_deleted[u] = -1
                        logger.error(
                            "Failed deleting duplicates for %s: %s", u, e, exc_info=True
                        )

                logger.info(
                    f"Deleted a total of {total_deleted} duplicate document(s)."
                )
                report["duplicates_deleted_count"] = total_deleted
                report["duplicates_deleted_per_uuid"] = per_uuid_deleted

    if args.report:
        write_report(report, Path(args.report))

    # cleanup
    try:
        db_handler.close()
    except Exception:
        pass

    # Exit code 0 for success, 1 if inconsistencies found (unless deletion removed them)
    inconsistencies = bool(
        report.get("lsdf_only") or report.get("invalid_archives") or insertion_errors
    )
    if args.delete_missing and report.get("deleted_count", 0) > 0:
        # removed DB-only entries; recompute db_only length for exit decision
        remaining_db_only = 0
        if report.get("db_only"):
            remaining_db_only = 0  # they were removed
        inconsistencies = bool(
            report["lsdf_only"]
            or report["invalid_archives"]
            or report["insertion_errors"]
            or remaining_db_only
        )

    # if duplicates were deleted, clear that class of inconsistency
    if args.delete_duplicates and report.get("duplicates_deleted_count", 0) > 0:
        # assume duplicates resolved
        inconsistencies = bool(
            report.get("lsdf_only")
            or report.get("invalid_archives")
            or insertion_errors
        )

    if inconsistencies:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
