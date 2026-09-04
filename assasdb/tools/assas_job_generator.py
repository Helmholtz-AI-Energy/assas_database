#!/usr/bin/env python
"""ASSAS Job Generator Script.

This script generates, submits, and manages jobs for converting ASTEC binary
archives.  Archive discovery is filesystem-driven; running this command does
not require MongoDB or a MongoDB backup.  A previously generated master-list
CSV can be supplied when rescanning the upload directory is undesirable.
"""

import os
import sys
import pandas as pd
import subprocess
import logging
import argparse
import shlex

from dataclasses import asdict
from enum import Enum
from pathlib import Path
from typing import List

try:
    from .assas_build_master_list import (
        DEFAULT_UPLOAD_DIRECTORY,
        PROCESSABLE_STATES,
        build_entries,
    )
except ImportError:  # Support direct execution: python assas_job_generator.py
    from assas_build_master_list import (  # type: ignore
        DEFAULT_UPLOAD_DIRECTORY,
        PROCESSABLE_STATES,
        build_entries,
    )

pd.set_option("display.max_rows", None)  # Show all rows
pd.set_option("display.max_columns", None)  # Show all columns
pd.set_option("display.width", None)  # Adjust width to avoid truncation

logger = logging.getLogger(__name__)


def result_path_for_upload(archive_path: str, upload_uuid: str) -> Path:
    """Return ``<upload-root>/<uuid>/result/dataset.h5`` for an archive."""
    parts = Path(archive_path).parts
    try:
        uuid_index = parts.index(str(upload_uuid))
    except ValueError as exception:
        raise ValueError(
            f"Archive path {archive_path!r} does not contain UUID {upload_uuid!r}."
        ) from exception

    return Path(*parts[: uuid_index + 1]) / "result" / "dataset.h5"


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


class AssasDocumentFileStatus(Enum):
    """Legacy status values used by the public dataframe helper functions.

    Keeping this small enum local prevents importing the database package just
    to parse command-line arguments or work with scheduler jobs.
    """

    UPLOADED = "Uploaded"
    CONVERTING = "Converting"
    VALID = "Valid"
    INVALID = "Invalid"


LIMIT_SAMPLES = 80000
BACKUP_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/backup_mongodb"
SLURM_ACCOUNT = os.environ.get("SLURM_ACCOUNT", "hk-project-pai00119")
SLURM_PARTITION = os.environ.get("SLURM_PARTITION", "cpuonly")
SLURM_TIME = os.environ.get("SLURM_TIME", "3-00:00:00")
MAIL_USER = os.environ.get("SLURM_MAIL_USER", "jonas.dressner@kit.edu")
TEMPLATE = """#!/bin/bash

# Training commands

#SBATCH --account={account}
#SBATCH --job-name={jobname}
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time={wall_time}
#SBATCH --mem=239400mb
#SBATCH --constraint=LSDF
#SBATCH --exclusive
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

srun python ${{PYDIR}}/assas_conversion_handler.py -uuid {uuid} {new_time_command} --log-level {log_level}
mv ../slurm-${{SLURM_JOBID}}.out ${{LOGDIR}}
mv ../slurm-error-${{SLURM_JOBID}}.out ${{LOGDIR}}
"""  # noqa: E501

BUNDLE_TEMPLATE = """#!/bin/bash

# Run several independent conversions on one exclusively allocated node.

#SBATCH --account={account}
#SBATCH --job-name=convert-bundle-{bundle_index:03d}
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --ntasks={bundle_size}
#SBATCH --time={wall_time}
#SBATCH --mem={memory_mb}mb
#SBATCH --constraint=LSDF
{exclusive_directive}
#SBATCH --output={job_directory}/slurm-%j.out
#SBATCH --error={job_directory}/slurm-error-%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jonas.dressner@kit.edu

module purge
source {env_dir}/bin/activate

export PYDIR={py_dir}
export ASTEC_ROOT={astec_root}
export LOGDIR={job_directory}/job_${{SLURM_JOB_ID}}
mkdir -p "${{LOGDIR}}"
cd "${{LOGDIR}}"

{conversion_steps}

failed=0
for pid in "${{pids[@]}}"; do
    if ! wait "$pid"; then
        failed=1
    fi
done
exit "$failed"
"""


MASTER_TEMPLATE = """#!/bin/bash

#SBATCH --account={account}
#SBATCH --job-name={jobname}
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time={wall_time}
#SBATCH --mem={memory_mb}mb
#SBATCH --constraint=LSDF
#SBATCH --exclusive
#SBATCH --output={job_directory}/slurm-%j.out
#SBATCH --error={job_directory}/slurm-error-%j.out
#SBATCH --mail-type=FAIL
#SBATCH --mail-user={mail_user}

set -o pipefail

module purge
source {env_dir}/bin/activate

export PYDIR={py_dir}
export ASTEC_ROOT={astec_root}
export LOGDIR={job_directory}/job_${{SLURM_JOB_ID}}
mkdir -p "${{LOGDIR}}"
cd "${{LOGDIR}}"

ARCHIVE="{archive_path}"
SOURCE_TAR="{source_tar}"
SCRATCH="${{TMPDIR:-/tmp}}/{uuid}"
INPUT_COPY_FLAG=""

cleanup() {{
    if [ -n "${{EXTRACTED_DIR:-}}" ] && [ -d "${{EXTRACTED_DIR}}" ]; then
        echo "Removing extracted archive ${{EXTRACTED_DIR}}"
        rm -rf "${{EXTRACTED_DIR}}"
    fi
}}
trap cleanup EXIT

# Preprocessing: extract the archive into node-local scratch when the upload
# was never unpacked.  Extracting here rather than onto LSDF keeps ~53 GB per
# archive off the shared filesystem, and replaces the handler's own copy step
# (the archive is already node-local afterwards, hence --no-input-copy).
if [ -n "${{SOURCE_TAR}}" ]; then
    if [ ! -f "${{SOURCE_TAR}}" ]; then
        echo "Source tarball ${{SOURCE_TAR}} does not exist" >&2
        exit 1
    fi

    mkdir -p "${{SCRATCH}}"
    echo "Extracting ${{SOURCE_TAR}} into ${{SCRATCH}}"
    if ! tar -xf "${{SOURCE_TAR}}" -C "${{SCRATCH}}"; then
        echo "Extraction of ${{SOURCE_TAR}} failed" >&2
        exit 1
    fi

    EXTRACTED_DIR="${{SCRATCH}}/{archive_basename}"
    if [ ! -d "${{EXTRACTED_DIR}}" ]; then
        echo "Extraction did not produce ${{EXTRACTED_DIR}}" >&2
        ls -la "${{SCRATCH}}" >&2
        exit 1
    fi

    ARCHIVE="${{EXTRACTED_DIR}}"
    INPUT_COPY_FLAG="--no-input-copy"
fi

srun python "${{PYDIR}}/assas_conversion_handler.py" \\
    --archive-path "${{ARCHIVE}}" \\
    --output-path "{result_path}" \\
    --name {name} \\
    --upload_uuid {uuid} \\
    --new {time_command} {anonymization_command}${{INPUT_COPY_FLAG}} \\
    --log-level {log_level}
"""  # noqa: E501


def load_master_list(
    master_list_path: str,
    states: List[str] = None,
) -> pd.DataFrame:
    """Load the master list produced by assas_build_master_list.py.

    The master list is derived from the filesystem rather than from MongoDB,
    so it covers archives the database never registered correctly.

    Args:
        master_list_path (str): Path to the master list CSV.
        states (List[str]): Optional filter on the state column.

    Returns:
        pd.DataFrame: The master list entries.

    """
    logger.info(f"Load master list from {master_list_path}.")
    entries = pd.read_csv(master_list_path, dtype=str).fillna("")

    required = {"system_upload_uuid", "system_path", "state"}
    missing = required - set(entries.columns)
    if missing:
        raise ValueError(f"Master list is missing column(s): {sorted(missing)}")

    logger.info(f"Master list contains {len(entries)} entries.")
    for state, count in entries["state"].value_counts().items():
        logger.info(f"  {count} entries in state {state}.")

    if states:
        entries = entries[entries["state"].isin(states)]
        logger.info(f"Entries after state filter {states}: {len(entries)}.")

    return entries


def load_filesystem_entries(
    upload_directory: str,
    states: List[str] = None,
) -> pd.DataFrame:
    """Discover convertible archives directly from the upload filesystem."""
    upload_path = Path(upload_directory)
    if not upload_path.is_dir():
        raise FileNotFoundError(f"Upload directory does not exist: {upload_path}")

    logger.info("Scanning upload directory %s.", upload_path)
    entries = pd.DataFrame(
        asdict(entry) for entry in build_entries(upload_path, with_samples=False)
    )

    if entries.empty:
        logger.warning("No archive entries found under %s.", upload_path)
        return pd.DataFrame(
            columns=[
                "system_upload_uuid",
                "meta_name",
                "meta_description",
                "system_path",
                "system_result",
                "system_number_of_samples",
                "source_tar",
                "state",
                "recorded_path",
                "resolution",
            ]
        )

    for state, count in entries["state"].value_counts().items():
        logger.info("  %d entries in state %s.", count, state)

    if states:
        entries = entries[entries["state"].isin(states)]
        logger.info("Entries after state filter %s: %d.", states, len(entries))

    return entries


def generate_master_job_files(
    job_directory: str,
    entries: pd.DataFrame,
    log_level: str = "WARNING",
    account: str = SLURM_ACCOUNT,
    partition: str = SLURM_PARTITION,
    wall_time: str = SLURM_TIME,
    memory_mb: int = 239400,
    maximum_index: int = None,
    anonymization_directory: str = None,
    mail_user: str = MAIL_USER,
) -> List[str]:
    """Generate one job script per master-list entry, with untar preprocessing.

    Args:
        job_directory (str): Directory the job scripts are written to.
        entries (pd.DataFrame): Master list entries to generate jobs for.
        log_level (str): Log level passed to the conversion handler.
        account (str): SLURM account.
        partition (str): SLURM partition.
        wall_time (str): SLURM wall-time limit.
        memory_mb (int): Memory requested per job, in MB.
        maximum_index (int): Optional limit on converted time points.
        anonymization_directory (str): Directory holding the anonymization
            configuration; output is raw when omitted.
        mail_user (str): Address for SLURM failure mail.

    Returns:
        List[str]: Paths of the generated job scripts.

    """
    anonymization_command = ""
    if anonymization_directory is not None:
        anonymization_path = os.path.abspath(anonymization_directory)
        required_files = (
            os.path.join(anonymization_path, "fp_anonymization.json"),
            os.path.join(anonymization_path, "anonymization.json"),
        )
        missing_files = [path for path in required_files if not os.path.isfile(path)]
        if missing_files:
            raise FileNotFoundError(
                "Missing required anonymization file(s): " + ", ".join(missing_files)
            )
        quoted_path = shlex.quote(anonymization_path)
        anonymization_command = f"--anonymization-dir {quoted_path} "
    else:
        logger.warning(
            "Generating jobs without anonymization; output will be raw."
        )

    generated_files = []
    py_dir = os.path.dirname(os.path.realpath(__file__))
    job_directory = os.path.abspath(job_directory)

    for _, entry in entries.iterrows():
        uuid = entry["system_upload_uuid"]
        archive_path = entry["system_path"]
        source_tar = entry.get("source_tar", "")
        # Derive this from the UUID directory instead of trusting an older
        # master list.  Archives can be nested arbitrarily deeply below their
        # upload, but all results belong directly below the UUID directory.
        result_path = result_path_for_upload(archive_path, uuid)

        script = MASTER_TEMPLATE.format(
            jobname=f"convert-{uuid}",
            account=account,
            partition=partition,
            wall_time=wall_time,
            memory_mb=memory_mb,
            job_directory=job_directory,
            py_dir=py_dir,
            env_dir=os.environ.get("VIRTUAL_ENV", ""),
            astec_root=os.environ.get("ASTEC_ROOT", ""),
            uuid=uuid,
            archive_path=archive_path,
            archive_basename=os.path.basename(archive_path),
            source_tar=source_tar,
            result_path=result_path,
            name=shlex.quote(str(entry.get("meta_name", "")) or uuid),
            time_command=f"--time {maximum_index}" if maximum_index is not None else "",
            anonymization_command=anonymization_command,
            log_level=log_level,
            mail_user=mail_user,
        )

        filename = os.path.join(job_directory, f"convert-{uuid}.sh")
        with open(filename, "w") as handle:
            handle.write(script)
        generated_files.append(filename)

    logger.info(f"Generated {len(generated_files)} job scripts in {job_directory}.")

    return generated_files


def submit_master_jobs(job_directory: str, maximum_jobs: int = None) -> int:
    """Submit the job scripts generated from the master list.

    Args:
        job_directory (str): Directory holding the job scripts.
        maximum_jobs (int): Optional limit on how many jobs to submit.

    Returns:
        int: Number of jobs submitted.

    """
    filenames = sorted(
        name
        for name in os.listdir(job_directory)
        if name.startswith("convert-") and name.endswith(".sh")
    )

    if maximum_jobs is not None:
        filenames = filenames[:maximum_jobs]

    submitted = 0
    for filename in filenames:
        result = subprocess.run(
            ["sbatch", os.path.join(job_directory, filename)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            logger.error(f"Failed to submit {filename}: {result.stderr.strip()}")
            continue
        submitted += 1
        logger.debug(f"Submitted {filename}: {result.stdout.strip()}")

    logger.info(f"Submitted {submitted} of {len(filenames)} jobs.")

    return submitted


def get_database_entries() -> pd.DataFrame:
    """Return all database entries from the backup directory.

    This function initializes an instance of `AssasDatabaseManager` with the
    specified backup directory and retrieves all database entries.
    """
    # Kept for callers of the old Python API. Database dependencies are lazy so
    # the generator CLI and filesystem workflow never import them.
    from assasdb import AssasDatabaseHandler, AssasDatabaseManager

    database_manager = AssasDatabaseManager(
        database_handler=AssasDatabaseHandler(
            client=None, backup_directory=BACKUP_DIRECTORY
        ),
    )

    logger.info(f"Get all database entries from backup directory: {BACKUP_DIRECTORY}.")
    database_entries = database_manager.get_all_database_entries_from_backup()
    logger.info(f"Number of database entries: {len(database_entries)}.")

    return database_entries


def get_database_sizes(
    database_entries: pd.DataFrame,
    key: str = "system_size",
) -> dict:
    """Return the sizes of database files after a certain status.

    This function calculates the sizes of database files based on their status
    and returns a dictionary with the status as keys and their corresponding sizes
    as values. It also includes a total size entry.

    Args:
        database_entries (pd.DataFrame): DataFrame containing database entries.
        key (str): The key to use for size calculation (default: "system_size").

    Returns:
        dict: A dictionary with status as keys and their sizes as values.

    """
    from assasdb import AssasDatabaseManager

    size_info = {}
    sum_sizes = 0
    for status in AssasDocumentFileStatus:
        size_tuple = AssasDatabaseManager.get_size_of_database_files_after_status(
            dataframe=database_entries[
                database_entries["system_status"] == status.value
            ],
            key=key,
        )
        logger.info(f"Size tuple for status {status.value}: {size_tuple}")

        # size_tuple expected: (human_readable, raw_bytes)
        human_readable = None
        raw_bytes = 0
        try:
            human_readable = size_tuple[0]
        except Exception:
            human_readable = None

        try:
            raw_bytes = size_tuple[1]
        except Exception:
            raw_bytes = 0

        # Ensure raw_bytes is an int
        try:
            raw_int = int(raw_bytes)
        except (TypeError, ValueError):
            logger.warning(
                f"Non-numeric raw size for status {status.value}: {raw_bytes!r}. "
                "Attempting to parse digits or using 0."
            )
            try:
                import re

                digits = re.sub(r"[^\d]", "", str(raw_bytes))
                raw_int = int(digits) if digits else 0
            except Exception:
                raw_int = 0

        size_info[status.value] = human_readable
        sum_sizes += raw_int

    size_info["total"] = AssasDatabaseManager.convert_from_bytes(sum_sizes)

    return size_info


def get_maximum_indizes(
    number_of_samples: int,
    limit_samples: int,
) -> List[int]:
    """Return a list of maximum indizes for the job parameter list.

    This function calculates the maximum indices based on the total number of samples
    and the limit for samples per job. It divides the total number of samples by the
    limit and creates a list of maximum indices, ensuring that the last index does not
    exceed the total number of samples.
    """
    leng_of_list = number_of_samples // limit_samples

    if number_of_samples % limit_samples != 0:
        leng_of_list = leng_of_list + 1

    maximum_indizes = list(range(0, leng_of_list))
    logger.debug(f"Number of maximum indizes: {len(maximum_indizes)}.")

    for i in range(len(maximum_indizes)):
        maximum_indizes[i] = limit_samples * (i + 1)

        if maximum_indizes[i] > number_of_samples:
            maximum_indizes[i] = number_of_samples

        logger.debug(f"File index {i} has maximum index {maximum_indizes[i]}.")

    return maximum_indizes


def get_job_parameter_list(
    entry: pd.Series,
    limit_samples: int,
    log_level: str = "WARNING",
) -> List[dict]:
    """Return a list of job parameters for the given entry.

    Each job parameter is a dictionary with the keys 'jobname', 'uuid'
    and 'new_time_command'.
    """
    job_parameter_list = []

    uuid = entry["system_upload_uuid"]
    maximum_indizes = get_maximum_indizes(
        number_of_samples=int(entry["system_number_of_samples"]),
        limit_samples=limit_samples,
    )

    if len(maximum_indizes) == 1:
        job_parameters = {
            "jobname": "convert-" + uuid,
            "account": SLURM_ACCOUNT,
            "partition": SLURM_PARTITION,
            "wall_time": SLURM_TIME,
            "py_dir": os.path.dirname(os.path.realpath(__file__)),
            "env_dir": os.environ.get("VIRTUAL_ENV", ""),
            "astec_root": os.environ.get("ASTEC_ROOT", ""),
            "uuid": uuid,
            "new_time_command": "-n",
            "log_level": log_level,
        }

        job_parameter_list.append(TEMPLATE.format(**job_parameters))

    if len(maximum_indizes) > 1:
        for i, _ in enumerate(maximum_indizes):
            job_parameters = {
                "jobname": "convert-" + uuid,
                "account": SLURM_ACCOUNT,
                "partition": SLURM_PARTITION,
                "wall_time": SLURM_TIME,
                "py_dir": os.path.dirname(os.path.realpath(__file__)),
                "env_dir": os.environ.get("VIRTUAL_ENV", ""),
                "astec_root": os.environ.get("ASTEC_ROOT", ""),
                "uuid": uuid,
                "new_time_command": f"-t {maximum_indizes[i]}",
                "log_level": log_level,
            }

            job_parameter_list.append(TEMPLATE.format(**job_parameters))

    return job_parameter_list


def generate_job_file(
    job_directory: str,
    entry: pd.Series,
    limit_samples: int,
    log_level: str = "WARNING",
) -> None:
    """Generate a job file for the given entry.

    The job file is saved in the jobs directory with the name 'convert-{uuid}.sh'.
    If there are multiple job parameters, it generates multiple job files with the
    name 'convert-{uuid}-{i}.sh'.
    """
    uuid = entry["system_upload_uuid"]
    number_of_samples = entry["system_number_of_samples"]

    logger.info(f"Generate job (upload_uuid = {uuid}, samples = {number_of_samples})")

    if number_of_samples is None or pd.isna(number_of_samples):
        logger.warning(f"Skipping {uuid} with NaN number of samples.")
        return

    if int(number_of_samples) < 0:
        print(f"Skipping {uuid} with negative number of samples: {number_of_samples}.")
        return

    job_parameter_list = get_job_parameter_list(
        entry=entry,
        limit_samples=limit_samples,
        log_level=log_level,
    )
    logger.debug(f"Job parameter list for {uuid}: {job_parameter_list}")

    if not job_parameter_list:
        logger.warning(f"No job parameter list for {uuid}.")
        return

    if len(job_parameter_list) == 1:
        job_parameters = job_parameter_list[0]

        logger.info(f"Only one job parameter for {uuid}.")
        logger.debug(f"Parameter: {job_parameters}.")

        with open(os.path.join(job_directory, f"convert-{uuid}.sh"), "w") as handle:
            handle.write(job_parameters)

    if len(job_parameter_list) > 1:
        logger.info(f"Multiple job parameters for {uuid}.")
        logger.debug(f"Job parameter list: {job_parameter_list}.")
        logger.info(f"Generate {len(job_parameter_list)} job files for {uuid}.")

        for i, job_parameters in enumerate(job_parameter_list):
            logger.debug(f"Job parameters: {job_parameters}.")
            filename = os.path.join(job_directory, f"convert-{uuid}-{i}.sh")

            with open(filename, "w") as handle:
                handle.write(job_parameters)


def generate_job_files(
    job_directory: str,
    database_entries: pd.DataFrame,
    limit_samples: int = LIMIT_SAMPLES,
    log_level: str = "WARNING",
) -> None:
    """Generate job files for all entries in the database with the status 'Uploaded'.

    It filters the database entries for those with the status 'Uploaded' and applies
    the generate_job_file function to each entry.
    """
    logger.info(f"Generate job files for {len(database_entries)} entries.")

    database_entries.apply(
        lambda entry: generate_job_file(job_directory, entry, limit_samples, log_level),
        axis=1,
    )


def generate_bundle_job_files(
    job_directory: str,
    database_entries: pd.DataFrame,
    bundle_size: int,
    log_level: str = "WARNING",
    account: str = SLURM_ACCOUNT,
    partition: str = SLURM_PARTITION,
    wall_time: str = SLURM_TIME,
    maximum_index: int = None,
    memory_mb: int = 239400,
    exclusive_node: bool = True,
    anonymization_directory: str = None,
) -> List[str]:
    """Generate scripts that run several grouped conversions per exclusive node."""
    if bundle_size < 2:
        raise ValueError("bundle_size must be at least 2")

    anonymization_command = ""
    if anonymization_directory is not None:
        anonymization_path = os.path.abspath(anonymization_directory)
        required_files = (
            os.path.join(anonymization_path, "fp_anonymization.json"),
            os.path.join(anonymization_path, "anonymization.json"),
        )
        missing_files = [path for path in required_files if not os.path.isfile(path)]
        if missing_files:
            raise FileNotFoundError(
                "Missing required anonymization file(s): " + ", ".join(missing_files)
            )
        anonymization_command = (
            f"--anonymization-dir {shlex.quote(anonymization_path)} "
        )
    else:
        logger.warning(
            "Generating bundle jobs without anonymization; output will be raw."
        )

    generated_files = []
    py_dir = os.path.dirname(os.path.realpath(__file__))
    entries = database_entries.reset_index(drop=True)

    for bundle_index, start in enumerate(range(0, len(entries), bundle_size)):
        bundle = entries.iloc[start : start + bundle_size]
        steps = ["pids=()"]
        for _, entry in bundle.iterrows():
            upload_uuid = entry["system_upload_uuid"]
            time_command = (
                f"--time {maximum_index}" if maximum_index is not None else ""
            )
            steps.extend(
                [
                    (
                        "srun --exclusive --nodes=1 --ntasks=1 --cpus-per-task=1 "
                        f"--mem={memory_mb // bundle_size}M "
                        f"python \"${{PYDIR}}/assas_conversion_handler.py\" "
                        f"--upload_uuid {upload_uuid} --new {time_command} "
                        f"{anonymization_command}"
                        f"--log-level {log_level} "
                        f"> \"${{LOGDIR}}/{upload_uuid}.out\" 2>&1 &"
                    ),
                    'pids+=("$!")',
                ]
            )

        script = BUNDLE_TEMPLATE.format(
            bundle_index=bundle_index,
            bundle_size=len(bundle),
            account=account,
            partition=partition,
            wall_time=wall_time,
            memory_mb=memory_mb,
            exclusive_directive="#SBATCH --exclusive" if exclusive_node else "",
            py_dir=py_dir,
            job_directory=os.path.abspath(job_directory),
            env_dir=os.environ.get("VIRTUAL_ENV", ""),
            astec_root=os.environ.get("ASTEC_ROOT", ""),
            conversion_steps="\n".join(steps),
        )
        filename = os.path.join(job_directory, f"convert-bundle-{bundle_index:03d}.sh")
        with open(filename, "w") as handle:
            handle.write(script)
        generated_files.append(filename)

    return generated_files


def submit_bundle_jobs(job_directory: str) -> None:
    """Submit all generated bundle scripts in lexical order."""
    filenames = sorted(
        name
        for name in os.listdir(job_directory)
        if name.startswith("convert-bundle-") and name.endswith(".sh")
    )
    for filename in filenames:
        subprocess.run(["sbatch", os.path.join(job_directory, filename)], check=True)


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


def extract_upload_uuid(job_name: str) -> str:
    """Extract the upload UUID from the job name.

    Assumes the job name contains the UUID in a specific format.
    For example, if the job name is "convert-<upload_uuid>", it extracts <upload_uuid>.
    """
    if "convert-" in job_name:
        return job_name.split("convert-")[1]

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
    limit_samples: int,
    single_jobs: bool = False,
    multi_jobs: bool = False,
) -> None:
    """Submit jobs for each entry in the database not in 'Valid' or 'Invalid' status.

    It checks the status of each entry and submits jobs accordingly.
    If there are multiple jobs for an entry, it sets dependencies between them.

    Args:
        database_entries (pd.DataFrame): DataFrame containing database entries.
        limit_samples (int): Maximum number of samples per job.
        single_jobs (bool): If True, allows single jobs for each entry.
        multi_jobs (bool): If True, allows multiple jobs for the same entry.

    Returns:
        None: This function does not return any value.

    """
    previous_job_id = None

    for _, database_entry in database_entries.iterrows():
        uuid = database_entry["system_upload_uuid"]

        if (
            database_entry["system_status"] == "Valid"
            or database_entry["system_status"] == "Invalid"
        ):
            logger.warning(
                f"Skipping {uuid} with status {database_entry['system_status']}"
            )
            continue

        number_of_samples = database_entry["system_number_of_samples"]

        if number_of_samples is None or pd.isna(number_of_samples):
            logger.warning(f"Skipping {uuid} with NaN number of samples.")
            continue

        if int(number_of_samples) < 0:
            logger.warning(
                f"Skipping {uuid} with negative number of samples: {number_of_samples}."
            )
            continue

        maximum_indizes = get_maximum_indizes(
            number_of_samples=int(number_of_samples),
            limit_samples=limit_samples,
        )

        filtered_maximum_indizes = []
        # Safely parse completed samples; treat NaN/None/non-int as 0
        completed_raw = database_entry.get("system_number_of_samples_completed")
        if pd.isna(completed_raw):
            completed_samples = 0
        else:
            try:
                completed_samples = int(completed_raw)
            except (TypeError, ValueError):
                logger.warning(
                    f"Non-integer completed samples for {uuid}: {completed_raw!r}. "
                    "Treating as 0."
                )
                completed_samples = 0

        # ensure non-negative
        if completed_samples < 0:
            completed_samples = 0

        for idx in maximum_indizes:
            if idx <= completed_samples:
                logger.info(
                    f"Skipping idx {idx} for {uuid} as it is already completed."
                )
                filtered_maximum_indizes.append(-1)
            else:
                filtered_maximum_indizes.append(idx)

        logger.debug(f"Filtered maximum indizes for {uuid}: {filtered_maximum_indizes}")

        if len(maximum_indizes) == 1:
            if multi_jobs:
                logger.info(
                    f"Skipping single job for {uuid} with {number_of_samples} samples."
                )
                continue
            if filtered_maximum_indizes[0] == -1:
                logger.info(
                    f"Skipping single job for {uuid} with {number_of_samples} "
                    "samples as it is already completed."
                )
                continue

            logger.info(
                f"No maximum indizes for {uuid} with {number_of_samples} samples."
            )
            logger.info(f"Submit job for {uuid} with {number_of_samples} samples.")
            submit_call = f"sbatch {os.path.dirname(os.path.realpath(__file__))}"
            submit_call += f"/jobs/convert-{uuid}.sh"

            logger.debug(f"Submit_call: {submit_call}")

            os.system(submit_call)

        if len(maximum_indizes) > 1:
            if single_jobs:
                logger.info(
                    f"Skipping multi-job for {uuid} with {number_of_samples} samples."
                )
                continue

            logger.info(f"Submit jobs for {uuid} with {number_of_samples} samples.")

            for i in range(len(maximum_indizes)):
                if filtered_maximum_indizes[i] == -1:
                    logger.info(
                        f"Skipping job index {i} for {uuid} with "
                        f"maximum index {maximum_indizes[i]} as it "
                        "is already completed."
                    )
                    previous_job_id = None
                    continue

                logger.info(
                    f"Submit job index {i} for {uuid} with "
                    f"maximum index {maximum_indizes[i]}."
                )

                submit_call = f"sbatch {os.path.dirname(os.path.realpath(__file__))}"
                submit_call += f"/jobs/convert-{uuid}-{i}.sh"

                if previous_job_id and i > 0:
                    submit_call = f"sbatch --dependency=afterok:{previous_job_id}"
                    submit_call += f" {os.path.dirname(os.path.realpath(__file__))}"
                    submit_call += f"/jobs/convert-{uuid}-{i}.sh"

                logger.debug(f"Submit_call: {submit_call}")

                result = subprocess.run(
                    submit_call, shell=True, stdout=subprocess.PIPE, text=True
                )

                logger.debug(
                    f"""Job submission details:
                        UUID: {uuid}
                        Index: {i}
                        Output: {result.stdout.strip()}"""
                )

                if result.returncode != 0:
                    logger.error(f"Error submitting job for {uuid}: {result.stderr}")
                    break

                previous_job_id = result.stdout.strip().split()[-1]


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


def get_job_dependencies(state: SlurmJobState) -> pd.DataFrame:
    """Retrieve the dependencies of all running SLURM jobs.

    Returns:
        pd.DataFrame: A DataFrame containing job IDs and their dependencies.

    """
    try:
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%j,%i,%t"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        df = pd.DataFrame(rows, columns=["job_name", "job_id", "status"])
        df["upload_uuid"] = df["job_name"].apply(extract_upload_uuid)

        jobs = df[df["status"] == state.value].copy()
        jobs["job_id"] = jobs["job_id"].astype(str)

        for job in jobs.itertuples():
            job_id = job.job_id
            index = job.Index
            scontrol_result = subprocess.run(
                ["scontrol", "show", "job", job_id],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )

            for line in scontrol_result.stdout.split("\n"):
                if "Dependency=" in line:
                    jobs.loc[index, "dependencies"] = line.split("Dependency=")[
                        1
                    ].strip()
                    break

        return jobs

    except subprocess.CalledProcessError as e:
        logger.error(f"Error while retrieving jobs or dependencies: {e.stderr}")
        return pd.DataFrame(columns=["job_id", "status", "dependencies"])


def _filter_entries(
    entries: pd.DataFrame,
    contains: str = None,
    upload_uuid: str = None,
    maximum_files: int = None,
) -> pd.DataFrame:
    """Apply the common, database-independent archive filters."""
    if contains is not None:
        entries = entries[
            entries["meta_name"].str.contains(contains, case=False, na=False)
        ]
    if upload_uuid is not None:
        entries = entries[entries["system_upload_uuid"] == upload_uuid]
    if maximum_files is not None:
        entries = entries.head(maximum_files)
    return entries


def main(argv: List[str] = None) -> int:
    """Run the filesystem-backed job-generator CLI."""
    parser = argparse.ArgumentParser(description="ASSAS Job Generator Script")
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
        choices=["generate", "submit", "cancel", "squeue", "dependencies"],
        help="Action to perform",
    )
    parser.add_argument(
        "--master-list",
        type=str,
        default=None,
        help=(
            "optional CSV produced by assas_build_master_list.py; when omitted, "
            "archives are discovered directly under --upload-directory"
        ),
    )
    parser.add_argument(
        "-u",
        "--upload-directory",
        default=DEFAULT_UPLOAD_DIRECTORY,
        help="filesystem directory holding the per-UUID upload folders",
    )
    parser.add_argument(
        "--states",
        type=str,
        nargs="+",
        default=list(PROCESSABLE_STATES),
        help="filesystem archive states to generate jobs for",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Limit how many generated scripts are submitted",
    )
    parser.add_argument(
        "--uuid",
        type=str,
        default=None,
        help="UUID of the archive to generate jobs for (optional)",
    )
    parser.add_argument(
        "--contains",
        type=str,
        default=None,
        help="Filter discovered entries by name (for example, 'CESAR')",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Limit processing to the first N eligible database entries",
    )
    parser.add_argument(
        "--bundle-size",
        type=int,
        default=1,
        help="Run this many conversions concurrently on one exclusive node",
    )
    parser.add_argument(
        "--account",
        default=SLURM_ACCOUNT,
        help="SLURM account used by generated job scripts",
    )
    parser.add_argument(
        "--partition",
        default=SLURM_PARTITION,
        help="SLURM partition used by generated job scripts",
    )
    parser.add_argument(
        "--wall-time",
        default=SLURM_TIME,
        help="SLURM wall-time limit used by generated job scripts",
    )
    parser.add_argument(
        "--maximum-index",
        type=int,
        default=None,
        help="Limit bundled conversions to the first N time points",
    )
    parser.add_argument(
        "--memory-mb",
        type=int,
        default=239400,
        help="Total memory requested by a bundled job, in MB",
    )
    parser.add_argument(
        "--shared-node",
        action="store_true",
        help="Do not request exclusive ownership of the allocated node",
    )
    parser.add_argument(
        "--anonymization-dir",
        default=None,
        help=(
            "directory containing fp_anonymization.json and anonymization.json; "
            "generated conversions are raw when omitted"
        ),
    )
    args = parser.parse_args(argv)

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger.info(f"Logging level set to: {logging.getLevelName(log_level)}")

    if args.action == "generate":
        os.makedirs(args.job_directory, exist_ok=True)
        entries = (
            load_master_list(args.master_list, states=args.states)
            if args.master_list
            else load_filesystem_entries(args.upload_directory, states=args.states)
        )
        entries = _filter_entries(
            entries,
            contains=args.contains,
            upload_uuid=args.uuid,
            maximum_files=args.max_files,
        )
        logger.info("Generating jobs for %d filesystem entries.", len(entries))
        remove_all_job_files(job_directory=args.job_directory)
        if args.bundle_size > 1:
            logger.warning(
                "--bundle-size is ignored for filesystem jobs; generating one "
                "direct conversion script per archive."
            )
        generate_master_job_files(
            job_directory=args.job_directory,
            entries=entries,
            log_level=args.job_log_level,
            account=args.account,
            partition=args.partition,
            wall_time=args.wall_time,
            memory_mb=args.memory_mb,
            maximum_index=args.maximum_index,
            anonymization_directory=args.anonymization_dir,
        )

    elif args.action == "submit":
        submit_master_jobs(args.job_directory, maximum_jobs=args.max_jobs)

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

    elif args.action == "dependencies":
        logger.info("Retrieving job dependencies...")
        running_dependencies = get_job_dependencies(SlurmJobState.RUNNING)
        logger.info(f"Running dependencies:\n{running_dependencies}")
        pending_dependencies = get_job_dependencies(SlurmJobState.PENDING)
        logger.info(f"Pending dependencies:\n{pending_dependencies}")
        completed_dependencies = get_job_dependencies(SlurmJobState.COMPLETED)
        logger.info(f"Completed dependencies:\n{completed_dependencies}")

    logger.info("Script execution completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
