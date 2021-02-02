#!/usr/bin/env python3
"""
Functions to run the indels pipeline on AWS for a list of runs
"""

# Standard imports
import argparse
import csv
import os
import re
import subprocess
from collections import defaultdict

# Local imports
from common_utils import (AWS_CMD, ERROR_FASTQ, ERROR_NONE, ERROR_RUN_NO_DATA,
                          ERROR_RUN_NO_SAMPLE, ERROR_RUN_UNPROCESSED, INFO,
                          RUN_ID, RUN_SAMPLES, WARNING, get_files_in_s3)

# Manifests
MANIFESTS = {
    'CG001Qv4': 'CG001v4.0_Amplicon_Manifest_Panel4.0.3_20181101.tsv',
    'CG001Qv5': 'CG001v5.1_Amplicon_Manifest_Panel5.1.12_20200911.tsv'
}
MANIFEST_KEY_LG = len(list(MANIFESTS.keys())[0])

# Default AWS parameters
AWS_QUEUE = 'cchauve-orchestration-default'
AWS_DEF = 'cchauve'
AWS_RM = ['aws', 's3', 'rm']


def get_runs_manifests_list(runs_csv_file):
    """
    Get from a CSV file containing runs ID and runs name a list of pairs
    (run ID, manifest)
    :param: runs_csv_file (str): path to the input CSV file
    :assumption: column 0 is run name, column 1 is run ID

    :return: list((str, str)): list (run_id, manifest)
    """
    result = []
    with open(runs_csv_file) as csvfile:
        runs_data = csv.reader(csvfile, delimiter=',')
        for row in runs_data:
            run_id = row[1]
            run_name = row[0]
            manifest = MANIFESTS[run_name[0:MANIFEST_KEY_LG]]
            result.append((run_id, manifest, run_name))
    return result


# Return codes for checking if a path is an input raw FASTQ file
CHECK_SAMPLE_OUT_FILE = 'file'  # checked path is a file within run directory
CHECK_SAMPLE_OUT_ERROR_DIR = 'not a sample dir'  # checked if sample directory
CHECK_SAMPLE_OUT_ERROR_FILE = 'not a raw fastq file'  # Sample dir. not a FASTQ
CHECK_SAMPLE_OUT_OK = 'raw fastq file'  # Sample directory and FASTQ file
FASTQ_EXT = '_001.fastq.gz'


def is_sample_file(file_path_split):
    """
    Checks if a path corresponds to an expected sample input file
    :param: file_path (list(str)): path of file in s3 bucket split by '/'
    :return: (int, str):
    field 1: see values above
    field 2: string where the error occured or pair (sample_id, FASTQ file) if
    the result is CHECK_SAMPLE_OUT_OK
    :assumption: sample input file is of the form
    input/run_id/<sample_id>/<sample_id>_L001_[R1,R2]_001.fastq.gz
    :assumption: sample_id is of the form *-X_SX where X is an integer
    :assumption: an input directory contains only directories for samples plus
    some configuration files
    """
    if len(file_path_split) != 4:
        return (CHECK_SAMPLE_OUT_FILE, '')
    else:
        sample_dir = file_path_split[2]
        sample_dir_suffix = sample_dir.split('-')[-1]
        if '_' not in sample_dir_suffix:
            return (CHECK_SAMPLE_OUT_ERROR_DIR, sample_dir)
        sample_dir_nb = sample_dir_suffix.split('_')
        if not sample_dir_nb[0].isdigit():
            return (CHECK_SAMPLE_OUT_ERROR_DIR, sample_dir)
        if sample_dir_nb[1][0] != 'S' or not sample_dir_nb[1][1:].isdigit():
            return (CHECK_SAMPLE_OUT_ERROR_DIR, sample_dir)
        sample_id = sample_dir  # Directory is a proper sample id
        fastq_pattern = re.compile(f"{sample_id}_L00._R[1-2]{FASTQ_EXT}")
        if fastq_pattern.match(file_path_split[3]) is None:
            return (CHECK_SAMPLE_OUT_ERROR_FILE, file_path_split[3])
        return (CHECK_SAMPLE_OUT_OK, (sample_id, file_path_split[3]))


def check_input_data(run_id, s3_bucket, log_file):
    """
    Checks that the input data for run_id exists and is composed of two raw
    FASTQ files per sample
    :param: run_id (str): run ID
    :param: s3_bucket (str): bucket containing the data (in input directory)
    :param: log_file (opened file): log file
    :return: bool: True if all samples have the expected files
    """
    prefix = f"input/{run_id}"
    s3_files = get_files_in_s3(prefix, s3_bucket)
    if s3_files is None:
        log_file.write(f"{WARNING}.{run_id}\t{ERROR_RUN_NO_DATA}\n")
        return False
    else:
        fastq_files = defaultdict(list)
        for file in s3_files:
            # Expected format: input/run_id/sample_id/gzipped_fastq_file
            s3_file_path = file.split('/')
            s3_check_sample = is_sample_file(s3_file_path)
            if s3_check_sample[0] not in [
                    CHECK_SAMPLE_OUT_OK, CHECK_SAMPLE_OUT_FILE
            ]:
                log_file.write(
                    f"{WARNING}:{run_id}\t{' '.join(s3_check_sample)}\n")
                return False
            if s3_check_sample[0] != CHECK_SAMPLE_OUT_FILE:
                fastq_files[s3_check_sample[1][0]].append(
                    s3_check_sample[1][1])
        sample_id_list = list(fastq_files.keys())
        if len(sample_id_list) == 0:
            log_file.write(f"{WARNING}:{run_id}\t{ERROR_RUN_NO_SAMPLE}\n")
            return False
        else:
            sample_id_correct = []
            for sample_id in sample_id_list:
                files_list = fastq_files[sample_id]
                files_nb = len(files_list)
                if files_nb % 2 != 0:
                    log_file.write(
                        f"{WARNING}:{run_id}:{sample_id}\t{ERROR_FASTQ}\n")
                    return False
                files_list.sort()
                for i in range(0, files_nb, 2):
                    R1, R2 = files_list[i], files_list[i + 1]
                    if R1.replace('_R1_', '') != R2.replace('_R2_', ''):
                        log_file.write(
                            f"{WARNING}:{run_id}:{sample_id}\t{ERROR_FASTQ}\n")
                        return False
                sample_id_correct.append(sample_id)
            log_file.write(
                f"{RUN_SAMPLES}:{run_id}\t{' '.join(sample_id_correct)}\n")
    return True


if __name__ == "__main__":
    """
    Checks the input data for a list of runs and submits AWS jobs for each
    valid run.

    Arguments:
    - runs_csv_file: CSV file with 2 fields <run_name>,<run_id>
      run_name is used to define the amplicon manifest
    - s3_input: S3 bucket containing the runs input data
      assumed structure of a run input directory:
      configuration files and set of subdirectories
      <s3_input>/input/<run_id>/<sample_id>/<sample_id>_L001_[R1,R2]_001.fastq.gz
    - branch: branch of the indels-pipeline repo to use (currently
      BOVERI-448-nf or BOVERI-515 for MSI amplicons)
    - s3_output: S3 bucket where to store the results
      results for run_id go into <s3_output>/<run_id>
      default value in nextflow.config
    - aws_def: --job-definition value for aws
      default value: cchauve (AWS_DEF)
    - aws_queue: queue to send the jobs to, --job-queue value for aws
      default value: cchauve-orchestration-default (AWS_QUEUE)
    - trace_path: optional parameters, if present, S3 directory where reports
      are written

    Checks the directory <s3_input>/input/<run_id> for each run and looks into
    every directory ending by -XX_SYY where XX and YY are integers that there
    are two gzipped raw FASTQ files
    Any run
    - with a directory whose name does not match the definition of a sample ID
    - with a sample directory without only the two FASTQ files
    is not processed.

    Generates a log file log/run_csv_file ".csv" replaced by ".log" indicating
    processed runs and unprocessed runs. Errors in the log file are prefixed by
    WARNING.
    """
    # Input file
    ARGS_RUNS_FILE = ['runs_csv_file', None, 'Runs CSV file']
    # S3 bucket where the input data is located
    ARGS_INPUT_BUCKET = ['s3_input', None, 'Input S3 bucket directory']
    # Branch to use
    ARGS_BRANCH = ['branch', None, 'indels-pipeline branch']
    # S3 directory where to store the results
    ARGS_OUTPUT_BUCKET = ['-o', '--s3_output', 'output S3 bucket directory']
    # AWS def
    ARGS_AWS_DEF = ['-d', '--aws_def', 'AWS definition']
    # AWS queue
    ARGS_AWS_QUEUE = ['-q', '--aws_queue', 'AWS queue']

    parser = argparse.ArgumentParser(description='Indels pipeline: run on AWS')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    parser.add_argument(ARGS_INPUT_BUCKET[0],
                        type=str,
                        help=ARGS_INPUT_BUCKET[2])
    parser.add_argument(ARGS_BRANCH[0], type=str, help=ARGS_BRANCH[2])
    parser.add_argument(ARGS_OUTPUT_BUCKET[0],
                        ARGS_OUTPUT_BUCKET[1],
                        type=str,
                        help=ARGS_OUTPUT_BUCKET[2])
    parser.add_argument(ARGS_AWS_DEF[0],
                        ARGS_AWS_DEF[1],
                        default=AWS_DEF,
                        type=str,
                        help=ARGS_AWS_DEF[2])
    parser.add_argument(ARGS_AWS_QUEUE[0],
                        ARGS_AWS_QUEUE[1],
                        default=AWS_QUEUE,
                        type=str,
                        help=ARGS_AWS_QUEUE[2])
    args = parser.parse_args()

    # Creating a log file located in the same directory than the YAML
    # configuration file and with the same name with .yaml replaced by .log
    _, run_file_name = os.path.split(args.runs_csv_file)
    log_file_path = os.path.join('log',
                                 run_file_name.replace('.csv', '_input.log'))
    log_file = open(log_file_path, 'w')

    runs_manifests_list = get_runs_manifests_list(args.runs_csv_file)
    for (run_id, manifest, run_name) in runs_manifests_list:
        log_file.write(f"{RUN_ID}:{run_id}.{run_name}\n")
        check_run = check_input_data(run_id, args.s3_input, log_file)
        if check_run:
            aws_cmd = ['aws', 'batch', 'submit-job']
            aws_cmd += ['--job-name', run_id]
            aws_cmd += ['--job-queue', args.aws_queue]
            aws_cmd += ['--job-definition', args.aws_def]
            aws_cmd += ['--container-overrides']
            cmd_options = ['command=contextual-genomics/indels-pipeline']
            cmd_options += ['\"-r\"', f"\"{args.branch}\""]
            cmd_options += ['\"--run_id\"', f"\"{run_id}\""]
            cmd_options += ['\"--manifest\"', f"\"{manifest}\""]
            cmd_options += ['\"--snpeff_path\"', '\"/opt/snpEff\"']
            cmd_options += ['\"--publish_dir_name\"', f"\"{run_id}\""]
            cmd_options += [
                '\"--input_dir\"', f"\"s3://{args.s3_input}/input/\""
            ]
            if args.s3_output is not None:
                # Otherwise it uses arams.output_dir from nextflow.config
                cmd_options += [
                    '\"--output_dir\"', f"\"s3://{args.s3_output}/\""
                ]
            cmd_options += ['\"-resume\"']
            aws_cmd += [','.join(cmd_options)]
            aws_cmd += ['--region', 'ca-central-1']
            log_file.write(f"{INFO}:{run_id}\t{ERROR_NONE}\n")
            log_file.write(f"{AWS_CMD}:{run_id}\t{' '.join(aws_cmd)}\n")
            subprocess.call(aws_cmd)
        else:
            log_file.write(f"{WARNING}:{run_id}\t{ERROR_RUN_UNPROCESSED}\n")
    log_file.close()
