"""CPU Time Analysis."""

#!/usr/bin/env python3
import math
import subprocess
import argparse
import re
import logging
import pandas as pd

from collections import defaultdict

from assasdb import AssasDatabaseHandler, AssasDatabaseManager

LIMIT_SAMPLES = 80000
BACKUP_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/backup_mongodb"

logger = logging.getLogger(__name__)


def totalcpu_to_seconds(totalcpu_str: str) -> int:
    """Convert Slurm TotalCPU string [[DD-]HH:]MM:SS to seconds."""
    totalcpu_str = totalcpu_str.strip()
    days = 0

    if "-" in totalcpu_str:
        days_part, hms_part = totalcpu_str.split("-")
        days = int(days_part)
    else:
        hms_part = totalcpu_str

    parts = hms_part.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = map(int, parts)
    elif len(parts) == 2:
        hours = 0
        minutes, seconds = map(int, parts)
    else:
        # nur Sekunden
        hours = 0
        minutes = 0
        seconds = int(parts[0])

    total_seconds = seconds + minutes * 60 + hours * 3600 + days * 24 * 3600
    return total_seconds


def parse_sacct_output(output: str, jobname_filter: str = None) -> list[dict]:
    """Parse output of the 'sacct' command and returns a list of job dictionaries."""
    jobs = []
    logger.debug("sacct output size (chars): %d", len(output or ""))
    for line in (output or "").strip().split("\n"):
        if not line:
            continue
        parts = line.strip().split("|")
        if len(parts) < 7:
            logger.debug("Skipping malformed sacct line: %s", line)
            continue

        jobid, jobname, account, elapsed_raw, alloc_cpus, total_cpu, cpu_time_raw = (
            parts[0:7]
        )
        logger.debug("Parsing line: %s -> parts: %s", line, parts)

        # skip .batch/.extern steps
        if jobid.endswith(".batch") or jobid.endswith(".extern"):
            logger.debug("Skipping .batch or .extern job %s", jobid)
            continue

        # optional JobName Filter
        if jobname_filter:
            if not re.match(jobname_filter, jobname):
                logger.debug(
                    f"JobName {jobname} does not match filter {jobname_filter}, "
                    "skipping"
                )
                continue

        # protect against unexpected split when jobname_filter is None or not present
        try:
            system_uuid = (
                jobname.split(jobname_filter)[1] if jobname_filter else jobname
            )
        except Exception:
            system_uuid = jobname

        try:
            elapsed_sec = float(elapsed_raw)
            cpu_time = float(cpu_time_raw)
            total_cpu_sec = totalcpu_to_seconds(total_cpu)
            alloc_cpus = int(alloc_cpus)
            jobs.append(
                {
                    "JobID": jobid,
                    "JobName": jobname,
                    "Account": account,
                    "ElapsedRaw": elapsed_sec,
                    "AllocCPUS": alloc_cpus,
                    "TotalCPU": total_cpu_sec,
                    "TotalCPURaw": total_cpu,
                    "SystemUUID": system_uuid,
                    "CPUTime": cpu_time,
                }
            )
            logger.debug(
                "Added job %s (Elapsed: %s, AllocCPUS: %s, TotalCPU: %s)",
                jobid,
                elapsed_sec,
                alloc_cpus,
                total_cpu_sec,
            )
        except ValueError:
            logger.debug("Failed to parse numeric fields for line: %s", line)
            continue
    return jobs


def get_sacct_data(
    start_date: str, end_date: str, jobname_filter: str = None
) -> list[dict]:
    """Retrieve Slurm accounting data for a specific date range and job name filter."""
    cmd = [
        "sacct",
        "-S",
        start_date,
        "-E",
        end_date,
        "--state=COMPLETED",
        # "--account=hk-project-p0024075",
        # "--name={jobname_filter}",
        "--format=JobID,JobName,Account,ElapsedRaw,AllocCPUS,TotalCPU,CPUTimeRaw",
        "--noheader",
        "--parsable2",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"sacct command failed:\n{result.stderr}")

    return parse_sacct_output(result.stdout, jobname_filter)


def find_duplicate_jobids(jobs: list[dict]) -> dict[str, list[int]]:
    """Find duplicate job IDs in the list of jobs."""
    index_map = defaultdict(list)
    for idx, j in enumerate(jobs):
        index_map[j["JobID"]].append(idx)
    duplicates = {jobid: idxs for jobid, idxs in index_map.items() if len(idxs) > 1}
    return duplicates


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

    logger.info(f"Get all database entries from backup directory: {BACKUP_DIRECTORY}.")
    database_entries = database_manager.get_all_database_entries_from_backup()
    logger.info(f"Number of database entries: {len(database_entries)}.")

    return database_entries


def calculate_number_of_jobs(backup_dataframe: pd.DataFrame) -> int:
    """Calculate the total number of jobs required based on the backup DataFrame."""
    number_of_jobs = 0
    for idx, row in backup_dataframe.iterrows():
        uuid = row["system_upload_uuid"]
        if row["system_number_of_samples"] is None or pd.isna(
            row["system_number_of_samples"]
        ):
            n = 0
        else:
            try:
                n = int(row["system_number_of_samples"])
            except (ValueError, TypeError):
                n = 0

        logger.debug(f"Job {idx}: UUID = {uuid}, Number of Samples = {n}")
        required_jobs = math.ceil(n / LIMIT_SAMPLES) if n > 0 else 0

        logger.debug(f"Required jobs for this entry: {required_jobs}")
        number_of_jobs += required_jobs

    logger.info(f"Total number of jobs required: {number_of_jobs}")
    return number_of_jobs


def calculate_cpu_stats(jobs: list[dict]) -> tuple:
    """Calculate CPU statistics for the given jobs."""
    for job in jobs:
        logger.debug(
            f"JobID: {job['JobID']}, JobName: {job['JobName']}, "
            f"Account: {job['Account']}, ElapsedRaw: {job['ElapsedRaw']}, "
            f"AllocCPUS: {job['AllocCPUS']}, TotalCPU: {job['TotalCPU']}, "
            f"TotalCPURaw: {job['TotalCPURaw']}, CPUTime: {job['CPUTime']}, "
            f"SystemUUID: {job['SystemUUID']}"
        )
        cpu_hours_per_job = job["ElapsedRaw"] * job["AllocCPUS"] / 3600
        logger.debug(f"  CPU hours for job: {cpu_hours_per_job:.2f}")
        job["CPUHours"] = cpu_hours_per_job
        cpu_days_per_job = cpu_hours_per_job / 24
        logger.debug(f"  CPU days for job: {cpu_days_per_job:.2f}")
        job["CPUDays"] = cpu_days_per_job

        cpu_hours_per_job_act = job["TotalCPU"] / 3600
        logger.debug(f"  Actual CPU hours for job: {cpu_hours_per_job_act:.2f}")
        job["CPUHoursActual"] = cpu_hours_per_job_act
        cpu_days_per_job_act = cpu_hours_per_job_act / 24
        logger.debug(f"  Actual CPU days for job: {cpu_days_per_job_act:.2f}")
        job["CPUDaysActual"] = cpu_days_per_job_act

    total_cpu_hours = sum(j["CPUHours"] for j in jobs)
    average_cpu_hours = total_cpu_hours / len(jobs) if jobs else 0
    total_cpu_days = total_cpu_hours / 24
    average_cpu_days = average_cpu_hours / 24

    total_act_cpu_hours = sum(j["CPUHoursActual"] for j in jobs)
    average_act_cpu_hours = total_act_cpu_hours / len(jobs) if jobs else 0
    total_act_cpu_days = total_act_cpu_hours / 24
    average_act_cpu_days = average_act_cpu_hours / 24

    return (
        total_cpu_hours,
        average_cpu_hours,
        total_cpu_days,
        average_cpu_days,
        total_act_cpu_hours,
        average_act_cpu_hours,
        total_act_cpu_days,
        average_act_cpu_days,
    )


def main() -> None:
    """Execute the main analysis."""
    parser = argparse.ArgumentParser(
        description="Slurm CPU-time analysis for completed jobs"
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--jobname", help="Optional regex filter for JobName (e.g., 'convert.*')"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    args = parser.parse_args()

    # configure logging
    numeric_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level, format="%(asctime)s %(levelname)s %(message)s"
    )
    logger = logging.getLogger(__name__)

    jobs = get_sacct_data(args.start, args.end, args.jobname)

    duplicates = find_duplicate_jobids(jobs)
    if duplicates:
        logger.info(f"Found {len(duplicates)} duplicate JobID(s). Examples:")
        for jobid, idxs in list(duplicates.items())[:10]:
            logger.info(f"  JobID {jobid} occurs {len(idxs)} times at indices {idxs}")
    else:
        logger.info("No duplicate JobIDs found.")

    (
        total_cpu_hours,
        avg_cpu_hours,
        total_cpu_days,
        avg_cpu_days,
        total_act_cpu_hours,
        avg_act_cpu_hours,
        total_act_cpu_days,
        avg_act_cpu_days,
    ) = calculate_cpu_stats(jobs)

    logger.info(f"Number of jobs analyzed: {len(jobs)}")
    logger.info(
        f"Total CPU time: {total_cpu_hours:.1f} h ({total_cpu_days:.1f} CPU-days)"
    )
    logger.info(f"Average per job: {avg_cpu_hours:.1f} h ({avg_cpu_days:.2f} CPU-days)")
    logger.info(
        f"Total Actual CPU time: {total_act_cpu_hours:.1f} h "
        f"({total_act_cpu_days:.1f} CPU-days)"
    )
    logger.info(
        f"Average Actual per job: {avg_act_cpu_hours:.1f} h "
        f"({avg_act_cpu_days:.2f} CPU-days)"
    )

    # Backup DataFrame
    backup_dataframe = get_database_entries()
    logger.info(f"Backup DataFrame shape: {backup_dataframe.shape}")

    # Sample/job calculation
    number_of_jobs = calculate_number_of_jobs(backup_dataframe)
    logger.info(f"Total number of jobs required for backup samples: {number_of_jobs}")


if __name__ == "__main__":
    main()
