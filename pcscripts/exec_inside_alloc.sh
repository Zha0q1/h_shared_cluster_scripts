#!/bin/bash

set -e

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
  -N | --nodes)
    num_nodes=$2
    shift 2
    ;;
  --container)
    container_name=$2
    shift 2
    ;;
  --script_dir)
    script_dir=$2
    shift 2
    ;;
  --master)
    master=$2
    shift 2
    ;;
  *)
    break
    ;;
  esac
done

export tasks_per_node=8
export num_tasks=$((tasks_per_node*num_nodes))
export OMPI_MCA_btl_tcp_if_exclude="docker0,lo"
export PMIX_MCA_gds=hash

export num_cpus=`srun -N 1 lscpu -p | tail -n 1 | cut -d ',' -f1`
export num_cpus=$((num_cpus+1))
export num_cpus_per_task=$((num_cpus/tasks_per_node))

export master=$master

srun nvidia-modprobe -u -c=0
srun -n $num_tasks --container-name ${container_name} \
  --container-mount-home --no-container-remap-root --container-workdir $HOME \
  --container-mounts ${script_dir}:/pcscripts,/scratch:/scratch,/fsx:/fsx \
  --cpus-per-task ${num_cpus_per_task} --mpi=pmix --unbuffered --label \
  /pcscripts/runwithenvvars \
  smddprun -c /opt/conda $@
