#!/bin/bash

# Training commands

#SBATCH --account=hk-project-pai00112
#SBATCH --job-name=convert-0a3654fb-17d3-4089-8d0c-8d6879a0e53c
#SBATCH --partition=cpuonly
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=3-00:00:00
#SBATCH --mem=239400mb
#SBATCH --constraint=LSDF
#SBATCH --output=/hkfs/work/workspace/scratch/ke4920-assas-hdf5/ke4920-assas-conv-1765764483/ke4920-assas-netcdf4-1760405583/ke4920-assas-hdf5/assas_database/assasdb/tools/result/slurm-%j.out
#SBATCH --error=/hkfs/work/workspace/scratch/ke4920-assas-hdf5/ke4920-assas-conv-1765764483/ke4920-assas-netcdf4-1760405583/ke4920-assas-hdf5/assas_database/assasdb/tools/result/slurm-error-%j.out
#SBATCH --mail-type=ALL
#SBATCH --mail-user=jonas.dressner@kit.edu

# Modules
module purge

source /hkfs/work/workspace/scratch/ke4920-assas-hdf5/ke4920-assas-conv-1765764483/ke4920-assas-netcdf4-1760405583/ke4920-assas-hdf5/venv_python3.11/bin/activate

export PYDIR=/hkfs/work/workspace/scratch/ke4920-assas-hdf5/ke4920-assas-conv-1765764483/ke4920-assas-netcdf4-1760405583/ke4920-assas-hdf5/assas_database/assasdb/tools
export LOGDIR=${PYDIR}/result/job_${SLURM_JOB_ID}
export ASTEC_ROOT=/hkfs/work/workspace/scratch/ke4920-assas-hdf5/ke4920-assas-conv-1765764483/ke4920-assas-netcdf4-1760405583/ke4920-assas-hdf5/astec/astecV3.1.2

mkdir ${LOGDIR}
cd ${LOGDIR}

srun python ${PYDIR}/assas_tar_generator.py -uuid 0a3654fb-17d3-4089-8d0c-8d6879a0e53c -n --log-level WARNING
mv ../slurm-${SLURM_JOBID}.out ${LOGDIR}
mv ../slurm-error-${SLURM_JOBID}.out ${LOGDIR}
