#!/usr/bin/env python
"""ASSAS Job Generator Script.

This script is designed to generate, submit, and manage jobs for tar
ASTEC binary archives. It provides functionality to create job files,
submit jobs to a SLURM scheduler, cancel running jobs, and retrieve job
information from the SLURM queue. The script uses the `AssasDatabaseManager`
to interact with the ASSAS database and manage document files. It supports
multiple job configurations based on the number of samples in the database
and allows for job dependencies to be set when submitting multiple jobs
for the same archive.
"""

import math
import os
import re
import pandas as pd
import subprocess
import logging
import argparse

from pathlib import Path
from enum import Enum

from assasdb import AssasDatabaseManager, AssasDocumentFileStatus, AssasDatabaseHandler

pd.set_option("display.max_rows", None)  # Show all rows
pd.set_option("display.max_columns", None)  # Show all columns
pd.set_option("display.width", None)  # Adjust width to avoid truncation

logger = logging.getLogger(__name__)


class SlurmJobState(Enum):
    """Enum representing SLURM job states.

    This enum defines the possible states a SLURM job can be in, such as
    PENDING, RUNNING, COMPLETED, CANCELLED, FAILED, TIMEOUT, NODE_FAIL,
    PREEMPTED, and SUSPENDED. Each state is represented by a two-letter code
    that corresponds to the SLURM job state codes.
    """

    PENDING = "PD"  # Job is waiting in the queue
    RUNNING = "R"  # Job is currently running
    COMPLETED = "CD"  # Job has completed successfully
    CANCELLED = "CA"  # Job was cancelled
    FAILED = "F"  # Job failed
    TIMEOUT = "TO"  # Job timed out
    NODE_FAIL = "NF"  # Job failed due to node failure
    PREEMPTED = "PR"  # Job was preempted
    SUSPENDED = "S"  # Job is suspended


TEMPLATE = """#!/bin/bash

# Training commands

#SBATCH --account=hk-project-pai00119
#SBATCH --job-name={jobname}
#SBATCH --partition=cpuonly
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=02:00:00
#SBATCH --mem=512mb
#SBATCH --constraint=LSDF
#SBATCH --output={py_dir}/result/slurm-%j.out
#SBATCH --error={py_dir}/result/slurm-error-%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jonas.dressner@kit.edu

# Modules
module purge

source {env_dir}/bin/activate

export PYDIR={py_dir}
export LOGDIR=${{PYDIR}}/result/job_${{SLURM_JOB_ID}}
export ASTEC_ROOT={astec_root}

mkdir ${{LOGDIR}}
cd ${{LOGDIR}}

srun python ${{PYDIR}}/assas_tar_generator.py --safe-tar-and-delete-from-db --uuid {uuid} --progress --log-level {log_level} --target-dir {target_dir}
mv ../slurm-${{SLURM_JOBID}}.out ${{LOGDIR}}
mv ../slurm-error-${{SLURM_JOBID}}.out ${{LOGDIR}}
"""  # noqa: E501


def get_database_entries() -> pd.DataFrame:
    """Return all database entries from the internal database.

    This function initializes an instance of `AssasDatabaseManager` with the
    specified internal database and retrieves all database entries.
    """
    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            connection_string=os.environ.get("CONNECTIONSTRING"),
            backup_directory=os.environ.get("BACKUP_DIRECTORY"),
            database_name=os.environ.get("MONGO_DB_NAME"),
        ),
    )

    logger.info("Get all database entries from internal database.")
    database_entries = database_manager.get_all_database_entries()
    logger.info(f"Number of database entries: {len(database_entries)}.")

    return database_entries


def get_job_parameter(
    entry: pd.Series,
    log_level: str = "WARNING",
) -> str:
    """Return a list of job parameters for the given entry.

    Each job parameter is a dictionary with the keys 'jobname', 'uuid'
    and 'new_time_command'.
    """
    uuid = entry["system_upload_uuid"]
    target_dir = Path(os.environ.get("UPLOAD_DIRECTORY", "")) / uuid
    job_parameters = {
        "jobname": "tar-" + uuid,
        "py_dir": os.path.dirname(os.path.realpath(__file__)),
        "env_dir": os.environ.get("VIRTUAL_ENV", ""),
        "astec_root": os.environ.get("ASTEC_ROOT", ""),
        "uuid": uuid,
        "log_level": log_level,
        "target_dir": target_dir,
    }

    return TEMPLATE.format(**job_parameters)


def generate_job_file(
    job_directory: str,
    entry: pd.Series,
    log_level: str = "WARNING",
) -> None:
    """Generate a job file for the given entry.

    The job file is saved in the jobs directory with the name 'tar-{uuid}.sh'.
    If there are multiple job parameters, it generates multiple job files with the
    name 'tar-{uuid}-{i}.sh'.
    """
    uuid = entry["system_upload_uuid"]

    logger.info(f"Generate job (upload_uuid = {uuid})")

    job_parameter = get_job_parameter(
        entry=entry,
        log_level=log_level,
    )
    logger.debug(f"Job parameter list for {uuid}: {job_parameter}")

    if not job_parameter:
        logger.warning(f"No job parameter list for {uuid}.")
        return

    logger.info(f"Only one job parameter for {uuid}.")
    logger.debug(f"Parameter: {job_parameter}.")

    with open(os.path.join(job_directory, f"tar-{uuid}.sh"), "w") as handle:
        handle.write(job_parameter)


def generate_job_files(
    job_directory: str,
    database_entries: pd.DataFrame,
    log_level: str = "WARNING",
) -> None:
    """Generate job files for all entries in the database with the status 'Uploaded'.

    It filters the database entries for those with the status 'Uploaded' and applies
    the generate_job_file function to each entry.
    """
    logger.info(f"Generate job files for {len(database_entries)} entries.")

    for _, entry in database_entries.iterrows():
        generate_job_file(job_directory, entry, log_level)


def cancel_all_jobs_in_certain_state(state: SlurmJobState) -> None:
    """Cancel all running jobs.

    This function retrieves the list of jobs in the specified state from the SLURM queue
    and cancels each job using the `scancel` command.

    Args:
        state (SlurmJobState): The state of the jobs to cancel (e.g.,
        SlurmJobState.RUNNING).

    Returns:
        None: This function does not return any value.

    """
    try:
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%i,%t"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        df = pd.DataFrame(rows, columns=["job_id", "status"])

        running_jobs = df[df["status"] == state.value]["job_id"]

        for job_id in running_jobs:
            subprocess.run(["scancel", job_id], check=True)
            logger.info(f"Cancelled job (job_id = {job_id}, state = {state.value})")

    except subprocess.CalledProcessError as e:
        logger.error(f"Error while retrieving or cancelling jobs: {e.stderr}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


def extract_upload_uuid(job_name: str) -> str | None:
    """Extract the upload UUID from the job name.

    Assumes the job name contains the UUID in a specific format.
    For example, if the job name is "tar-<upload_uuid>", it extracts <upload_uuid>.
    """
    if "tar-" in job_name:
        return job_name.split("tar-")[1]

    logger.warning(f"Could not extract upload UUID from job name: {job_name}")

    return None


def get_squeue_dataframe() -> pd.DataFrame:
    """Retrieve job IDs and statuses using the `squeue` command.

    This function runs the `squeue` command to get the list of jobs in the SLURM queue,
    extracts relevant information such as job name, job ID, status code, and status,
    and returns it as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing job information with columns:
            - job_name: Name of the job
            - job_id: ID of the job
            - status_code: Status code of the job
            - status: Human-readable status of the job
            - upload_uuid: Extracted UUID from the job name

    """
    try:
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%j,%i,%t,%T"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        if len(rows) == 0 or (len(rows) == 1 and rows[0] == [""]):
            logger.info("No jobs found in squeue.")
            return pd.DataFrame(columns=["job_name", "job_id", "status_code", "status"])

        df = pd.DataFrame(rows, columns=["job_name", "job_id", "status_code", "status"])
        df["upload_uuid"] = df["job_name"].apply(extract_upload_uuid)

        return df

    except subprocess.CalledProcessError as e:
        logger.error(f"Error while running squeue: {e.stderr}")
        return pd.DataFrame(columns=["job_name", "job_id", "status_code", "status"])


def submit_jobs(
    database_entries: pd.DataFrame,
) -> None:
    """Submit jobs for each entry in the database not in 'Valid' or 'Invalid' status.

    It checks the status of each entry and submits jobs accordingly.
    If there are multiple jobs for an entry, it sets dependencies between them.

    Args:
        database_entries (pd.DataFrame): DataFrame containing database entries.

    Returns:
        None: This function does not return any value.

    """
    for _, database_entry in database_entries.iterrows():
        uuid = database_entry["system_upload_uuid"]

        if database_entry["system_status"] != AssasDocumentFileStatus.VALID.value:
            logger.warning(
                f"Skipping {uuid} with status {database_entry['system_status']}"
            )
            continue

        logger.info(f"Submit job for {uuid}.")
        submit_call = f"sbatch {os.path.dirname(os.path.realpath(__file__))}"
        submit_call += f"/jobs/tar-{uuid}.sh"

        logger.debug(f"Submit_call: {submit_call}")

        os.system(submit_call)


def remove_all_job_files(job_directory: str) -> None:
    """Remove all job files in the specified job directory.

    Args:
        job_directory (str): Path to the directory containing job files.

    """
    try:
        job_files = os.listdir(job_directory)

        for job_file in job_files:
            file_path = os.path.join(job_directory, job_file)
            os.remove(file_path)
            logger.info(f"Removed job file: {file_path}")

    except FileNotFoundError:
        logger.error(f"Job directory not found: {job_directory}")
    except Exception as e:
        logger.error(f"Error while removing job files: {e}")


def _has_radar_dataset_id(value: object) -> bool:
    """Return True if value is a non-empty radar_dataset_id (not None/NaN/'')."""
    if value is None:
        return False
    # pandas NaN handling
    try:
        if pd.isna(float(str(value))):
            return False
    except (ValueError, TypeError):
        pass
    s = str(value).strip()
    if not s:
        return False
    if s.lower() in {"nan", "none", "null"}:
        return False
    return True


def _slurm_mem_to_mb(v: object) -> float | None:
    """Convert Slurm memory strings (e.g. '1234K', '512M', '1.5G', '0', '') to MB."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {"0", "Unknown", "N/A"}:
        return None

    m = re.fullmatch(
        r"(?P<num>\d+(?:\.\d+)?)(?P<unit>[KMGTP]?)", s, flags=re.IGNORECASE
    )
    if not m:
        return None

    num = float(m.group("num"))
    unit = (m.group("unit") or "").upper()

    # Slurm commonly reports RSS in K/M/G
    if unit == "" or unit == "M":
        return num
    if unit == "K":
        return num / 1024.0
    if unit == "G":
        return num * 1024.0
    if unit == "T":
        return num * 1024.0 * 1024.0
    if unit == "P":
        return num * 1024.0 * 1024.0 * 1024.0
    return None


def get_running_jobs_memory(job_name_prefix: str = "tar-") -> pd.DataFrame:
    """Return current memory usage for RUNNING jobs (best-effort).

    Uses:
      - `squeue` to find RUNNING jobs
      - `sstat` to read *current* MaxRSS/AveRSS/MaxVMSize for each job step (.batch)

    Notes:
      - `sstat` only works for RUNNING jobs and may return empty output depending on
        cluster config/accounting.
      - Memory values are returned in MB (float) where available.

    Args:
        job_name_prefix: Only include jobs whose name starts with this prefix.

    Returns:
        pd.DataFrame with columns:
          job_id, job_name, max_rss, ave_rss, max_vmsize,
          max_rss_mb, ave_rss_mb, max_vmsize_mb, recommended_mem_mb

    """
    try:
        sq = subprocess.run(
            ["squeue", "--noheader", "--format=%i,%j,%t,%M"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("squeue failed: %s", (e.stderr or "").strip())
        return pd.DataFrame(
            columns=[
                "job_id",
                "job_name",
                "status_code",
                "elapsed",
                "max_rss",
                "ave_rss",
                "max_vmsize",
                "max_rss_mb",
                "ave_rss_mb",
                "max_vmsize_mb",
                "recommended_mem_mb",
            ]
        )

    rows = [line.split(",") for line in sq.stdout.strip().split("\n") if line.strip()]
    if not rows:
        return pd.DataFrame(
            columns=[
                "job_id",
                "job_name",
                "status_code",
                "elapsed",
                "max_rss",
                "ave_rss",
                "max_vmsize",
                "max_rss_mb",
                "ave_rss_mb",
                "max_vmsize_mb",
                "recommended_mem_mb",
            ]
        )

    df = pd.DataFrame(rows, columns=["job_id", "job_name", "status_code", "elapsed"])
    df = df[df["status_code"] == SlurmJobState.RUNNING.value]
    if job_name_prefix:
        df = df[df["job_name"].astype(str).str.startswith(job_name_prefix)]

    max_rss_list: list[str | None] = []
    ave_rss_list: list[str | None] = []
    max_vmsize_list: list[str | None] = []

    for job_id in df["job_id"].tolist():
        # `sstat` for running jobs; .batch is typically the relevant step
        try:
            st = subprocess.run(
                [
                    "sstat",
                    "-j",
                    f"{job_id}.batch",
                    "--noheader",
                    "--format=MaxRSS,AveRSS,MaxVMSize",
                    "-P",  # parsable, '|' delimiter
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
            out = (st.stdout or "").strip()
            if not out:
                max_rss_list.append(None)
                ave_rss_list.append(None)
                max_vmsize_list.append(None)
                continue

            # Example parsable output: "123456K|78910K|2345678K"
            parts = out.split("|")
            max_rss_list.append(parts[0].strip() if len(parts) > 0 else None)
            ave_rss_list.append(parts[1].strip() if len(parts) > 1 else None)
            max_vmsize_list.append(parts[2].strip() if len(parts) > 2 else None)

        except subprocess.CalledProcessError as e:
            logger.debug(
                "sstat failed for job_id=%s: %s", job_id, (e.stderr or "").strip()
            )
            max_rss_list.append(None)
            ave_rss_list.append(None)
            max_vmsize_list.append(None)

    df["max_rss"] = max_rss_list
    df["ave_rss"] = ave_rss_list
    df["max_vmsize"] = max_vmsize_list

    df["max_rss_mb"] = df["max_rss"].apply(_slurm_mem_to_mb)
    df["ave_rss_mb"] = df["ave_rss"].apply(_slurm_mem_to_mb)
    df["max_vmsize_mb"] = df["max_vmsize"].apply(_slurm_mem_to_mb)

    # Simple recommendation: 50% headroom + 512 MB buffer (tune as needed)
    def _recommend(mb: float | None) -> int | None:
        # pandas may pass NaN here (float), which is not None
        if mb is None:
            return None
        try:
            if pd.isna(mb):
                return None
        except Exception:
            pass
        return int(mb * 1.5 + 512)

    df["recommended_mem_mb"] = df["max_rss_mb"].apply(_recommend)
    return df


def _slurm_elapsed_to_seconds(v: object) -> int | None:
    """Parse Slurm elapsed strings like 'MM:SS', 'HH:MM:SS', 'D-HH:MM:SS' to seconds."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in {"0", "Unknown", "N/A"}:
        return None

    # D-HH:MM:SS
    m = re.fullmatch(r"(?P<d>\d+)-(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})", s)
    if m:
        return (
            int(m.group("d")) * 86400
            + int(m.group("h")) * 3600
            + int(m.group("m")) * 60
            + int(m.group("s"))
        )

    # HH:MM:SS
    m = re.fullmatch(r"(?P<h>\d{1,2}):(?P<m>\d{2}):(?P<s>\d{2})", s)
    if m:
        return int(m.group("h")) * 3600 + int(m.group("m")) * 60 + int(m.group("s"))

    # MM:SS
    m = re.fullmatch(r"(?P<m>\d{1,3}):(?P<s>\d{2})", s)
    if m:
        return int(m.group("m")) * 60 + int(m.group("s"))

    return None


def _seconds_to_slurm_time(seconds: int) -> str:
    """Format seconds to Slurm time string 'D-HH:MM:SS' (or 'HH:MM:SS' if < 1 day)."""
    seconds = int(max(0, seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    if d > 0:
        return f"{d}-{h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"


def _recommend(mb: float | None) -> int | None:
    """Calculate recommended memory in MB based on observed usage.

    Simple recommendation: 50% headroom + 512 MB buffer (tune as needed).
    """
    if mb is None:
        return None
    try:
        if pd.isna(mb):
            return None
    except Exception:
        pass
    return int(mb * 1.5 + 512)


def get_finished_jobs_usage(
    job_name_prefix: str = "tar-",
    *,
    limit: int = 200,
) -> pd.DataFrame:
    """Evaluate resource usage of finished jobs via `sacct` (uses steps for MaxRSS)."""
    try:
        p = subprocess.run(
            [
                "sacct",
                "-n",
                "-P",
                "--format=JobID,JobName,State,Elapsed,Timelimit,ReqMem,MaxRSS",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("sacct failed: %s", (e.stderr or "").strip())
        return pd.DataFrame()

    lines = [ln for ln in (p.stdout or "").splitlines() if ln.strip()]
    rows: list[dict] = []
    for ln in lines:
        parts = ln.split("|")
        if len(parts) < 7:
            continue
        job_id, job_name, state, elapsed, timelimit, req_mem, max_rss = [
            x.strip() for x in parts[:7]
        ]
        if state.startswith("RUNNING") or state.startswith("PENDING"):
            continue
        rows.append(
            {
                "job_id": job_id,
                "job_name": job_name,
                "state": state,
                "elapsed": elapsed,
                "timelimit": timelimit,
                "req_mem": req_mem,
                "max_rss": max_rss,
            }
        )

    if not rows:
        return pd.DataFrame()

    raw = pd.DataFrame(rows)
    raw["job_id_root"] = raw["job_id"].astype(str).str.split(".").str[0]
    raw["is_root"] = ~raw["job_id"].astype(str).str.contains(r"\.")
    raw["is_batch"] = raw["job_id"].astype(str).str.endswith(".batch")

    # IMPORTANT: filter by ROOT job_name, but keep all steps for those roots
    roots = raw[raw["is_root"]]
    if job_name_prefix:
        keep_roots = set(
            roots[roots["job_name"].astype(str).str.startswith(job_name_prefix)][
                "job_id_root"
            ].tolist()
        )
        raw = raw[raw["job_id_root"].isin(keep_roots)]

    raw["max_rss_mb"] = raw["max_rss"].apply(_slurm_mem_to_mb)
    raw["elapsed_seconds"] = raw["elapsed"].apply(_slurm_elapsed_to_seconds)

    # Base info per root job
    base = (
        raw[raw["is_root"]]
        .sort_values(["job_id_root"], ascending=[False])
        .drop_duplicates(subset=["job_id_root"], keep="first")
        .drop(columns=["job_id"])
        .rename(columns={"job_id_root": "job_id"})
    )

    # Memory: prefer batch if present else any step;
    # also compute any-step max (covers ".0 python")
    mem_batch = (
        raw[raw["is_batch"]]
        .groupby("job_id_root", as_index=False)["max_rss_mb"]
        .max()
        .rename({"job_id_root": "job_id", "max_rss_mb": "max_rss_mb_batch"})
    )
    mem_any = (
        raw.groupby("job_id_root", as_index=False)["max_rss_mb"]
        .max()
        .rename({"job_id_root": "job_id", "max_rss_mb": "max_rss_mb_any"})
    )

    df = base.merge(mem_batch, on="job_id", how="left").merge(
        mem_any, on="job_id", how="left"
    )
    df["max_rss_mb"] = df["max_rss_mb_batch"].combine_first(df["max_rss_mb_any"])
    df["recommended_mem_mb"] = df["max_rss_mb"].apply(_recommend)
    df["recommended_time_seconds"] = df["elapsed_seconds"].apply(
        lambda sec: int(math.ceil(sec * 1.5))
        if sec is not None and not pd.isna(sec)
        else None
    )
    df["recommended_time"] = df["recommended_time_seconds"].apply(
        lambda s: _seconds_to_slurm_time(s) if s is not None else None
    )

    # keep most recent jobs
    try:
        df["_job_id_int"] = df["job_id"].astype(int)
        df = df.sort_values("_job_id_int", ascending=False).drop(
            columns=["_job_id_int"]
        )
    except Exception:
        pass

    return df.head(int(limit))


def count_entries_by_status(
    database_entries: pd.DataFrame, status: AssasDocumentFileStatus
) -> int:
    """Count the number of entries in the database with the given status.

    Args:
        database_entries (pd.DataFrame): DataFrame containing database entries.
        status (AssasDocumentFileStatus): The status to count entries for.

    Returns:
        int: The count of entries with the specified status.

    """
    return len(database_entries[database_entries["system_status"] == status.value])


def _tar_file_exists(entry: pd.Series) -> bool:
    """Return True if a tar file already exists for the given entry.

    Checks if a .tar file with the same name as the archive directory
    exists at the system_path location.

    Args:
        entry: A row from the database entries DataFrame.

    Returns:
        True if the tar file already exists, False otherwise.

    """
    system_path = str(entry.get("system_path", "") or "").strip()
    if not system_path:
        logger.warning(
            "No system_path for uuid=%s, skipping tar check.",
            entry.get("system_upload_uuid", "?"),
        )
        return False

    archive_dir = Path(system_path)
    # tar file is expected next to the directory with the same name + .tar
    tar_file = archive_dir.parent / f"{archive_dir.name}.tar"

    if tar_file.exists():
        logger.debug(
            "Tar file already exists for uuid=%s: %s",
            entry.get("system_upload_uuid", "?"),
            tar_file,
        )
        return True

    logger.debug(
        "No tar file found for uuid=%s at expected path: %s",
        entry.get("system_upload_uuid", "?"),
        tar_file,
    )
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ASSAS Tar Job Generator Script")
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: WARNING)",
    )
    parser.add_argument(
        "--job_log_level",
        type=str,
        default="WARNING",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: WARNING)",
    )
    parser.add_argument(
        "--job_directory",
        type=str,
        default=os.path.join(os.path.dirname(os.path.realpath(__file__)), "jobs"),
        help="Path to the job directory",
    )
    parser.add_argument(
        "--action",
        type=str,
        required=True,
        choices=["generate", "submit", "cancel", "squeue", "memory"],
        help="Action to perform: generate, submit, cancel, squeue, or memory",
    )
    parser.add_argument(
        "--state",
        type=str,
        default=AssasDocumentFileStatus.VALID.value,
        choices=[
            AssasDocumentFileStatus.VALID.value,
            AssasDocumentFileStatus.INVALID.value,
            AssasDocumentFileStatus.CONVERTING.value,
            AssasDocumentFileStatus.UPLOADED.value,
        ],
        help=f"State of the jobs to cancel \
        (default: {AssasDocumentFileStatus.VALID.value})",
    )
    parser.add_argument(
        "--uuid",
        type=str,
        default=None,
        help="UUID of the archive to generate or submit jobs for (optional)",
    )
    parser.add_argument(
        "--astec_root",
        type=str,
        default=os.environ.get("ASTEC_ROOT"),
        help="Path to the ASTEC root directory",
    )
    parser.add_argument(
        "--env_dir",
        type=str,
        default=os.environ.get("VIRTUAL_ENV"),
        help="Path to the virtual environment directory",
    )
    parser.add_argument(
        "--job-name-prefix",
        default="tar-",
        help="Job name prefix filter for measure actions (default: tar-).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max number of finished jobs to show for 'measure-finished'.",
    )
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger.info(f"Logging level set to: {logging.getLevelName(log_level)}")

    job_log_level = getattr(logging, args.job_log_level.upper(), logging.WARNING)
    logger.info(f"Job logging level set to: {logging.getLevelName(job_log_level)}")

    logger.info(f"Parsed actions: {args.action}")

    if not os.path.exists(args.job_directory):
        logger.info(
            f"Job directory '{args.job_directory}' does not exist. Creating it..."
        )
        os.makedirs(args.job_directory, exist_ok=True)

    database_entries = get_database_entries()

    for status in AssasDocumentFileStatus.__members__.values():
        count = count_entries_by_status(database_entries, status)
        logger.info(f"Number of archives in state {status} in database: {count}.")

    logger.info(f"All archives in database: {len(database_entries)}.")

    file_status_list = [AssasDocumentFileStatus(args.state)]
    logger.info(f"File status list: {file_status_list}.")

    file_status_value_list = [status.value for status in file_status_list]
    logger.info(
        f"Generate job files for entries with status: {file_status_value_list}."
    )
    database_entries = database_entries[
        database_entries["system_status"].isin(file_status_value_list)
    ]

    logger.info(f"Generate job files for {len(database_entries)} entries.")

    # If a UUID is provided, filter the database entries for that specific UUID
    if args.uuid is not None:
        logger.info(f"Filtering database entries by UUID: {args.uuid}.")
        database_entries = database_entries[
            database_entries["system_upload_uuid"] == args.uuid
        ]

    if "radar_dataset_id" not in getattr(database_entries, "columns", []):
        logger.warning(
            "Column 'radar_dataset_id' not present in database entries; "
            "cannot filter. No jobs will be generated."
        )
        database_entries = database_entries.iloc[0:0]
    else:
        before = len(database_entries)
        database_entries = database_entries[
            database_entries["radar_dataset_id"].apply(_has_radar_dataset_id)
        ]
        logger.info(
            "Filtered entries by radar_dataset_id: %d -> %d",
            before,
            len(database_entries),
        )

    # Filter out entries where tar file already exists
    before = len(database_entries)
    database_entries = database_entries[
        ~database_entries.apply(_tar_file_exists, axis=1)
    ]
    logger.info(
        "Filtered entries without existing tar file: %d -> %d",
        before,
        len(database_entries),
    )

    logger.info(f"Filtered database entries: {len(database_entries)}.")
    logger.info(f"Generating job files for {len(database_entries)} entries.")

    if args.action == "generate":
        logger.info(f"Generating job files into {args.job_directory}.")

        remove_all_job_files(job_directory=args.job_directory)

        generate_job_files(
            job_directory=args.job_directory,
            database_entries=database_entries,
            log_level=args.job_log_level,
        )

    elif args.action == "submit":
        logger.info("Submitting jobs...")

        submit_jobs(database_entries=database_entries)

    elif args.action == "cancel":
        logger.info("Cancelling all jobs in certain states...")
        # cancel_all_jobs_in_certain_state(SlurmJobState.RUNNING)
        cancel_all_jobs_in_certain_state(SlurmJobState.PENDING)

    elif args.action == "squeue":
        logger.info("Retrieving squeue DataFrame...")
        squeue_df = get_squeue_dataframe()
        logger.info(f"Squeue DataFrame:\n{squeue_df}")
        running_jobs = len(
            squeue_df[squeue_df["status_code"] == SlurmJobState.RUNNING.value]
        )
        pending_jobs = len(
            squeue_df[squeue_df["status_code"] == SlurmJobState.PENDING.value]
        )
        completed_jobs = len(
            squeue_df[squeue_df["status_code"] == SlurmJobState.COMPLETED.value]
        )
        logger.info(
            f"Information from squeue:\n"
            f"Total number of jobs in squeue: {len(squeue_df)}.\n"
            f"Number of running jobs in squeue: {running_jobs}.\n"
            f"Number of pending jobs in squeue: {pending_jobs}.\n"
            f"Number of completed jobs in squeue: {completed_jobs}."
        )

    elif args.action == "memory":
        logger.info("Retrieving memory usage for RUNNING jobs...")
        mem_df = get_running_jobs_memory(job_name_prefix="tar-")
        logger.info(f"Memory usage for RUNNING jobs:\n{mem_df}")
        df = get_finished_jobs_usage(
            job_name_prefix=args.job_name_prefix, limit=args.limit
        )
        if df.empty:
            print("No finished jobs found (or sacct has no data).")
        else:
            cols = [
                "job_id",
                "job_name",
                "state",
                "elapsed",
                "max_rss",
                "max_rss_mb",
                "recommended_mem_mb",
                "recommended_time",
            ]
            cols = [c for c in cols if c in df.columns]
            print(df[cols].to_string(index=False))

    else:
        logger.error(
            f"Invalid action: {args.action}. "
            "Choose from 'generate', 'submit', 'cancel', 'squeue', or 'memory'."
        )

    logger.info("Script execution completed.")
