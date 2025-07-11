#!/usr/bin/env python
"""ASSAS Job Generator Script.

This script is designed to generate, submit, and manage jobs for converting
ASTEC binary archives. It provides functionality to create job files,
submit jobs to a SLURM scheduler, cancel running jobs, and retrieve job
information from the SLURM queue. The script uses the `AssasDatabaseManager`
to interact with the ASSAS database and manage document files. It supports
multiple job configurations based on the number of samples in the database
and allows for job dependencies to be set when submitting multiple jobs
for the same archive.
"""

import os
import pandas as pd
import subprocess
import logging
import argparse

from enum import Enum
from typing import List

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


LIMIT_SAMPLES = 80000
BACKUP_DIRECTORY = "/lsdf/kit/scc/projects/ASSAS/backup_mongodb"
TEMPLATE = """#!/bin/bash

# Training commands

#SBATCH --account=hk-project-p0024075
#SBATCH --job-name={jobname}
#SBATCH --partition=cpuonly
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=3-00:00:00
#SBATCH --mem=239400mb
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

srun python ${{PYDIR}}/assas_conversion_handler.py -uuid {uuid} {new_time_command}
mv ../slurm-${{SLURM_JOBID}}.out ${{LOGDIR}}
mv ../slurm-error-${{SLURM_JOBID}}.out ${{LOGDIR}}
"""  # noqa: E501


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

        # logger.debug(f"File index {i} has maximum index {maximum_indizes[i]}.")

    return maximum_indizes


def get_job_parameter_list(
    entry: pd.Series,
    limit_samples: int,
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
            "py_dir": os.path.dirname(os.path.realpath(__file__)),
            "env_dir": os.environ.get("VIRTUAL_ENV", ""),
            "astec_root": os.environ.get("ASTEC_ROOT", ""),
            "uuid": uuid,
            "new_time_command": "-n",
        }

        job_parameter_list.append(TEMPLATE.format(**job_parameters))

    if len(maximum_indizes) > 1:
        for i, _ in enumerate(maximum_indizes):
            if i == 0:
                job_parameters = {
                    "jobname": "convert-" + uuid,
                    "py_dir": os.path.dirname(os.path.realpath(__file__)),
                    "env_dir": os.environ.get("VIRTUAL_ENV", ""),
                    "astec_root": os.environ.get("ASTEC_ROOT", ""),
                    "uuid": uuid,
                    "new_time_command": f"-n -t {maximum_indizes[0]}",
                }

            if i > 0:
                job_parameters = {
                    "jobname": "convert-" + uuid,
                    "py_dir": os.path.dirname(os.path.realpath(__file__)),
                    "env_dir": os.environ.get("VIRTUAL_ENV", ""),
                    "astec_root": os.environ.get("ASTEC_ROOT", ""),
                    "uuid": uuid,
                    "new_time_command": f"-t {maximum_indizes[i]}",
                }

            job_parameter_list.append(TEMPLATE.format(**job_parameters))

    return job_parameter_list


def generate_job_file(
    job_directory: str,
    entry: pd.Series,
    limit_samples: int,
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
    # file_status_list: List[AssasDocumentFileStatus] = [
    #    AssasDocumentFileStatus.UPLOADED
    # ],
    limit_samples: int = LIMIT_SAMPLES,
) -> None:
    """Generate job files for all entries in the database with the status 'Uploaded'.

    It filters the database entries for those with the status 'Uploaded' and applies
    the generate_job_file function to each entry.
    """
    # file_status_value_list = [status.value for status in file_status_list]
    # logger.info(
    #    f"Generate job files for entries with status: {file_status_value_list}."
    # )
    # database_entries = database_entries[
    #    database_entries["system_status"].isin(file_status_value_list)
    # ]
    logger.info(f"Generate job files for {len(database_entries)} entries.")

    database_entries.apply(
        lambda entry: generate_job_file(job_directory, entry, limit_samples), axis=1
    )


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
        # Get the list of jobs and their statuses
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%i,%t"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        # Parse the output into a list of rows
        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        # Convert the rows into a DataFrame
        df = pd.DataFrame(rows, columns=["job_id", "status"])

        # Filter for running jobs (status 'R')
        running_jobs = df[df["status"] == state.value]["job_id"]

        # Cancel each running job
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
        return job_name.split("convert-")[1]  # Extract the UUID

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
        # Run the squeue command and capture its output
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%j,%i,%t,%T"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        # Parse the output into a list of rows
        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        if len(rows) == 0 or (len(rows) == 1 and rows[0] == [""]):
            logger.info("No jobs found in squeue.")
            return pd.DataFrame(columns=["job_name", "job_id", "status_code", "status"])

        # Convert the rows into a DataFrame
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

        if len(maximum_indizes) == 1:
            if multi_jobs:
                logger.info(
                    f"Skipping single job for {uuid} with {number_of_samples} samples."
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

                previous_job_id = result.stdout.strip().split()[
                    -1
                ]  # Extract job ID from sbatch output


def remove_all_job_files(job_directory: str) -> None:
    """Remove all job files in the specified job directory.

    Args:
        job_directory (str): Path to the directory containing job files.

    """
    try:
        # List all files in the job directory
        job_files = os.listdir(job_directory)

        # Remove each job file
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
        # Get the list of jobs and their statuses
        result = subprocess.run(
            ["squeue", "--noheader", "--format=%j,%i,%t"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

        # Parse the output into a list of rows
        rows = [line.split(",") for line in result.stdout.strip().split("\n")]

        # Convert the rows into a DataFrame
        df = pd.DataFrame(rows, columns=["job_name", "job_id", "status"])
        df["upload_uuid"] = df["job_name"].apply(extract_upload_uuid)

        # Filter for running jobs (status 'R')
        jobs = df[df["status"] == state.value].copy()
        jobs["job_id"] = jobs["job_id"].astype(str)  # Ensure job_id is string type

        # Fetch dependencies for each running job
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ASSAS Job Generator Script")
    parser.add_argument(
        "-d", "--debug", action="store_true", help="Enable debug logging"
    )
    parser.add_argument(
        "-b",
        "--backup_directory",
        type=str,
        default=BACKUP_DIRECTORY,
        help="Path to the backup directory containing the database entries",
    )
    parser.add_argument(
        "-l",
        "--limit_samples",
        type=int,
        default=LIMIT_SAMPLES,
        help="Maximum number of samples per job (default: 80000)",
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
        # nargs="+",  # Allow multiple actions
        choices=["generate", "submit", "cancel", "squeue", "dependencies"],
        help="Action to perform: generate, submit, cancel, or dependencies",
    )
    parser.add_argument(
        "--state",
        type=str,
        default="Uploaded",
        choices=[
            AssasDocumentFileStatus.VALID.value,
            AssasDocumentFileStatus.INVALID.value,
            AssasDocumentFileStatus.CONVERTING.value,
            AssasDocumentFileStatus.UPLOADED.value,
        ],
        help=f"State of the jobs to cancel \
        (default: {AssasDocumentFileStatus.UPLOADED.value})",
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
        "-m", "--multiple", action="store_true", help="Submit only multi-jobs"
    )
    parser.add_argument(
        "-s", "--single", action="store_true", help="Submit only single-jobs"
    )
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    logger.info(f"Parsed actions: {args.action}")

    # Ensure the job directory exists
    if not os.path.exists(args.job_directory):
        logger.info(
            f"Job directory '{args.job_directory}' does not exist. Creating it..."
        )
        os.makedirs(args.job_directory, exist_ok=True)

    # Get all database entries
    database_entries = get_database_entries()

    statuses = [
        AssasDocumentFileStatus.VALID,
        AssasDocumentFileStatus.INVALID,
        AssasDocumentFileStatus.CONVERTING,
        AssasDocumentFileStatus.UPLOADED,
    ]

    for status in statuses:
        count = count_entries_by_status(database_entries, status)
        logger.info(f"{status} archives in database: {count}.")

    logger.info(f"All archives in database: {len(database_entries)}.")

    if args.action == "generate":
        logger.info(f"Generating job files into {args.job_directory}.")
        # database_entries = database_entries[
        #    database_entries["system_upload_uuid"] ==
        #    "9660f85e-ed19-400f-952f-0ee36d4c50c6"]

        # logger.info(database_entries)

        remove_all_job_files(job_directory=args.job_directory)

        if args.state is not None:
            state_enum = AssasDocumentFileStatus(args.state)
            file_status_list = [state_enum]
            logger.info(f"Filtering database entries by state: {args.state}.")
        else:
            file_status_list = [
                # AssasDocumentFileStatus.VALID,
                # AssasDocumentFileStatus.INVALID,
                # AssasDocumentFileStatus.CONVERTING,
                AssasDocumentFileStatus.UPLOADED,
            ]
        logger.info(f"File status list: {file_status_list}.")

        file_status_value_list = [status.value for status in file_status_list]
        logger.info(
            f"Generate job files for entries with status: {file_status_value_list}."
        )
        database_entries = database_entries[
            database_entries["system_status"].isin(file_status_value_list)
        ]

        logger.info(f"Generate job files for {len(database_entries)} entries.")

        if args.uuid is not None:
            logger.info(f"Filtering database entries by UUID: {args.uuid}.")
            database_entries = database_entries[
                database_entries["system_upload_uuid"] == args.uuid
            ]

        logger.info(f"Filtered database entries: {len(database_entries)}.")
        logger.info(f"Generating job files for {len(database_entries)} entries.")

        generate_job_files(
            job_directory=args.job_directory,
            database_entries=database_entries,
            # file_status_list=[AssasDocumentFileStatus.VALID],
            limit_samples=args.limit_samples,
        )

    elif args.action == "submit":
        logger.info("Submitting jobs...")
        database_entries = database_entries[
            database_entries["system_upload_uuid"]
            == "9660f85e-ed19-400f-952f-0ee36d4c50c6"
        ]

        logger.info(database_entries)

        submit_jobs(
            database_entries=database_entries,
            limit_samples=args.limit_samples,
            single_jobs=args.single,
            multi_jobs=args.multiple,
        )

    elif args.action == "cancel":
        logger.info("Cancelling all jobs in certain states...")
        cancel_all_jobs_in_certain_state(SlurmJobState.RUNNING)
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

    else:
        logger.error(
            f"Invalid action: {args.action}. "
            "Choose from 'generate', 'submit', 'cancel', or 'dependencies'."
        )

    logger.info("Script execution completed.")
