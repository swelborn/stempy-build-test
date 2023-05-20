from pathlib import Path
import httpx
import sfapi_client
import click
import httpx
import asyncio

from typing import List
from jinja2 import Template
from sfapi_client.compute import Machine
from sfapi_client import AsyncClient
from typing import Optional
import asyncio

# @click.command()
# @click.option('--git-tag', '-t', type=str, help='Specific git tag to use.')
async def main(git_tag: Optional[str] = None):
    # If no git_tag provided, use the latest from repo
    if git_tag is None:
        async with httpx.AsyncClient() as client:
            repo_name = "swelborn/stempy"
            # branch_name = "new-docker-gha"
            branch_name = "stempy-python-fix"
            commit_url = f"https://api.github.com/repos/{repo_name}/commits/{branch_name}"
            response = await client.get(commit_url) 
            response.raise_for_status()
            commit = response.json()
            git_tag = commit["sha"][:7]

    # 2. Build the tags up one by one
    # python_versions = ['37', '38', '39', '310']
    python_versions = ['310']

    mpi_statuses = ['ON', 'OFF']
    base_images = ['bionic', 'jammy']
    ipykernels = ['', 'ipykernel']
    # devs = ['', 'dev']
    devs = ['dev']

    tags: List[str] = []
    for py_ver in python_versions:
        for base_image in base_images:
            for dev in devs:
                dev_suffix = f"-{dev}" if dev else ""
                tag = f"samwelborn/stempy-mpi:py{py_ver}-{base_image}-{git_tag}{dev_suffix}"
                tags.append(tag)

    # Prepare jinja template
    template = """#!/bin/bash
#SBATCH --constraint=cpu
#SBATCH --nodes=2
#SBATCH --time=00:20:00
#SBATCH --job-name={{job_name}}
#SBATCH --exclusive
#SBATCH --qos=debug
#SBATCH --account=m3795
#SBATCH --chdir=/pscratch/sd/s/swelborn/2022.10.17
#SBATCH --output=/pscratch/sd/s/swelborn/2022.10.17/{{job_name}}/job.out
#SBATCH --error=/pscratch/sd/s/swelborn/2022.10.17/{{job_name}}/job.err

mkdir -p /pscratch/sd/s/swelborn/2022.10.17/{{job_name}}

export HDF5_USE_FILE_LOCKING=FALSE

error_exit()
{
echo "$1" 1>&2
exit 1
}

# Pull shifter image in from dockerhub
shifterimg pull {{image_name}}
echo {{job_name}}

# Remove any // ($DW_JOB_STRIPED has a trailing slash)
DATA_DIR=`echo "/pscratch/sd/s/swelborn/2022.10.17" |  sed 's/\/\//\//g'`
srun --exclusive -c 128 -n 2 shifter --image={{image_name}} \
    python3 /global/homes/s/swelborn/stempy_containers_tests/electron_count_cori.py \
    -w $DATA_DIR/{{job_name}} --pad \
    -l $DATA_DIR -t 4 -s 26 --multi-pass || error_exit "Error shifter execution failed."
# rm ${DATA_DIR}/{{job_name}}/data_scan*.h5
"""
    jinja_template = Template(template)

    async def submit_job(semaphore, tag):
        async with semaphore:
            job_name = tag.split(":")[1]
            image_name = tag
            script = jinja_template.render(job_name=job_name, image_name=image_name)
            keypath = Path("X")
            async with AsyncClient(key=keypath) as client:
                perlmutter = await client.compute(Machine.perlmutter)
                print(script)
                job = await perlmutter.submit_job(script)
                await job.complete()

    semaphore = asyncio.Semaphore(4)

    tasks = [submit_job(semaphore, tag) for tag in tags]

    await asyncio.gather(*tasks)

if __name__ == '__main__':
    import sys
    # We use asyncio.run here because click doesn't support async functions natively.
    git_tag = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main(git_tag))
