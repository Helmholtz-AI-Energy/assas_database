"""Set constant values in a CSV (optionally filtered); optionally also write to MongoDB.

Usage examples:
    # Set system_path and system_size_hdf5 for all rows in input.csv,
    # write to output.csv
"""

#!/usr/bin/env python3
import argparse
import ast
import logging
import math
import os
import re
import sys
from typing import Any, Generator

import pandas as pd

try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

try:
    from bson import ObjectId
except Exception:
    ObjectId = None


logger = logging.getLogger(__name__)


def setup_logging(verbose: bool) -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def env_default(*names: str, default: str = "") -> str:
    """Return first non-empty env var value from names, else default."""
    for n in names:
        v = os.getenv(n)
        if v is not None and str(v).strip() != "":
            return str(v)
    return default


def parse_kv(items: list[str]) -> dict[str, object]:
    """Parse repeated --set COL=VALUE into a dict with basic type coercion."""
    out: dict[str, object] = {}
    for it in items:
        if "=" not in it:
            raise SystemExit(f"--set expects COL=VALUE, got: {it!r}")
        k, v = it.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            raise SystemExit(f"Empty column name in --set {it!r}")

        if v.lower() in {"null", "none"}:
            out[k] = None
        elif v.lower() == "true":
            out[k] = True
        elif v.lower() == "false":
            out[k] = False
        else:
            try:
                out[k] = ast.literal_eval(v)
            except Exception:
                out[k] = v
    return out


def coerce_object_id(x: object) -> object:
    """Coerce a 24-hex string to bson.ObjectId if bson is available.

    Note:
        else return as-is.

    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if ObjectId is None:
        return x
    if isinstance(x, ObjectId):
        return x
    s = str(x).strip()
    if re.fullmatch(r"[0-9a-fA-F]{24}", s):
        return ObjectId(s)
    return x


def chunked(lst: list[Any], n: int) -> Generator[list[Any], None, None]:
    """Yield successive n-sized chunks from lst.

    Returns:
        Generator of lists, each with up to n items from lst.

    Example:
        chunked([1,2,3,4,5], 2) -> [[1,2], [3,4], [5]]

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def build_mask(df: pd.DataFrame, *, uuid: str, uuid_col: str, where: str) -> pd.Series:
    """Build a boolean mask for filtering the DataFrame."""
    if uuid and where:
        raise ValueError("Use either --uuid OR --where (not both).")

    if uuid:
        if uuid_col not in df.columns:
            raise KeyError(f"uuid column {uuid_col!r} not found in CSV")
        return df[uuid_col].astype(str).str.strip() == str(uuid).strip()

    if where:
        mask = df.eval(where)
        if getattr(mask, "dtype", None) is not bool:
            raise ValueError("--where must evaluate to a boolean mask")
        return mask

    return pd.Series([True] * len(df), index=df.index)


def main(argv: list[str]) -> int:
    """Execute the main program logic."""
    env_mongo_uri = env_default("CONNECTIONSTRING", "MONGO_URI", default="")
    env_db = env_default("MONGO_DB", default="")
    env_collection = env_default("MONGO_COLLECTION", default="")
    env_uuid_col = env_default("UUID_COL", default="uuid")
    env_mongo_uuid_field = env_default("MONGO_UUID_FIELD", default="")
    env_id_col = env_default("MONGO_ID_COL", default="_id")
    env_batch = int(env_default("MONGO_BATCH", default="2000"))

    ap = argparse.ArgumentParser(
        description=(
            "Set constant values in a CSV (optionally filtered); "
            "optionally also write to MongoDB.",
        )
    )
    ap.add_argument("--csv", required=True, help="Input CSV path")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging")

    ap.add_argument(
        "--set",
        action="append",
        default=[],
        help=(
            "Set a column to a constant value, "
            "e.g. --set system_path='/data/x' (repeatable).",
        ),
    )

    # Filtering options
    ap.add_argument(
        "--uuid",
        default="",
        help="If set, select rows by UUID value (simpler than --where).",
    )
    ap.add_argument(
        "--uuid-col",
        default=env_uuid_col,
        help=(
            "CSV column name that contains the UUID (default: env UUID_COL or 'uuid').",
        ),
    )
    ap.add_argument(
        "--where",
        default="",
        help=(
            "Optional pandas expression to filter rows, "
            "e.g. \"system_size == '0.0 B'\"",
        ),
    )

    # Optional MongoDB write-back (env defaults)
    ap.add_argument(
        "--mongo-uri",
        default=env_mongo_uri,
        help="Mongo URI (default: env CONNECTIONSTRING or MONGO_URI).",
    )
    ap.add_argument(
        "--db", default=env_db, help="Mongo DB name (default: env MONGO_DB)."
    )
    ap.add_argument(
        "--collection",
        default=env_collection,
        help="Mongo collection (default: env MONGO_COLLECTION).",
    )

    ap.add_argument(
        "--mongo-uuid-field",
        default=env_mongo_uuid_field,
        help=(
            "Mongo field name to match UUID "
            "(default: env MONGO_UUID_FIELD; else same as --uuid-col). "
            "Only used with --uuid.",
        ),
    )

    # Fallback: update Mongo by ids from CSV
    ap.add_argument(
        "--id-col",
        default=env_id_col,
        help=(
            "CSV column name for Mongo document _id field "
            "(default: env MONGO_ID_COL or '_id'). "
            "Only used when not using --uuid.",
        ),
    )
    ap.add_argument(
        "--mongo-batch",
        type=int,
        default=env_batch,
        help="How many ids per Mongo update_many() batch",
    )
    ap.add_argument(
        "--dry-run-mongo",
        action="store_true",
        help="Log what would be updated in MongoDB (no writes)",
    )

    args = ap.parse_args(argv)
    setup_logging(args.verbose)

    if not args.set:
        logger.error("Provide at least one --set COL=VALUE")
        return 2

    updates = parse_kv(args.set)

    df = pd.read_csv(args.csv)
    logger.info(
        "Loaded CSV rows=%d cols=%d from %s", len(df), len(df.columns), args.csv
    )

    # Ensure columns exist (create if missing)
    for col in updates.keys():
        if col not in df.columns:
            df[col] = pd.NA

    try:
        mask = build_mask(df, uuid=args.uuid, uuid_col=args.uuid_col, where=args.where)
    except (ValueError, KeyError) as e:
        logger.error(str(e))
        return 2
    except Exception as e:
        logger.error("Failed to build filter mask: %s", e)
        return 2

    changed_rows = int(mask.sum())
    if changed_rows == 0:
        logger.info("No CSV rows matched; nothing to change.")
    else:
        for col, val in updates.items():
            df.loc[mask, col] = val
        logger.info(
            "CSV updated rows=%d columns=%s", changed_rows, list(updates.keys())
        )

    df.to_csv(args.out, index=False)
    logger.info("Wrote %s", args.out)

    # Optional Mongo update
    wants_mongo = bool(
        (args.mongo_uri or "").strip()
        or (args.db or "").strip()
        or (args.collection or "").strip()
    )
    if not wants_mongo:
        return 0

    if not (args.mongo_uri and args.db and args.collection):
        logger.error(
            "To update MongoDB, provide --mongo-uri, --db, "
            "and --collection (or set env vars).",
        )
        return 2
    if MongoClient is None:
        logger.error("pymongo not installed. Install with: pip install pymongo")
        return 2

    client = MongoClient(args.mongo_uri)
    collection = client[args.db][args.collection]
    logger.info(
        "Mongo target: uri=%s db=%s collection=%s",
        args.mongo_uri,
        args.db,
        args.collection,
    )

    # Update Mongo by uuid directly
    if args.uuid:
        mongo_uuid_field = args.mongo_uuid_field.strip() or args.uuid_col.strip()
        if args.dry_run_mongo:
            logger.info(
                "Mongo DRY RUN: filter=%s=%r $set=%s",
                mongo_uuid_field,
                args.uuid,
                updates,
            )
            return 0

        res = collection.update_one(
            {mongo_uuid_field: args.uuid}, {"$set": updates}, upsert=False
        )
        logger.info(
            "Mongo update_one: matched=%d modified=%d",
            res.matched_count,
            res.modified_count,
        )
        if res.matched_count == 0:
            logger.error(
                "Mongo: no document matched that uuid. "
                "Check --mongo-uuid-field / --uuid.",
            )
            return 2
        return 0

    # Otherwise update by ids from CSV
    if args.id_col not in df.columns:
        logger.error("Id column %r not in CSV (needed for Mongo update).", args.id_col)
        return 2

    ids_series = df.loc[mask, args.id_col]
    ids: list[Any] = []
    for v in ids_series.tolist():
        if pd.isna(v) or str(v).strip() == "":
            continue
        ids.append(coerce_object_id(v) if args.id_col == "_id" else v)

    # De-dup while preserving order
    seen = set()
    uniq_ids: list[Any] = []
    for i in ids:
        key = str(i)
        if key in seen:
            continue
        seen.add(key)
        uniq_ids.append(i)

    if not uniq_ids:
        logger.info("Mongo: no ids found to update (after filtering).")
        return 0

    if args.dry_run_mongo:
        logger.info(
            "Mongo DRY RUN: would update id_col=%s ids=%d $set=%s",
            args.id_col,
            len(uniq_ids),
            updates,
        )
        return 0

    matched_total = 0
    modified_total = 0
    for chunk in chunked(uniq_ids, max(1, int(args.mongo_batch))):
        filt = {args.id_col: {"$in": chunk}}
        res = collection.update_many(filt, {"$set": updates})
        matched_total += res.matched_count
        modified_total += res.modified_count

    logger.info(
        "Mongo update_many: matched_total=%d modified_total=%d ids=%d",
        matched_total,
        modified_total,
        len(uniq_ids),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
