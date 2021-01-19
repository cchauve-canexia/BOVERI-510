#!/usr/bin/env python3
"""
Functions to run the indels pipeline on AWS for a list of runs
"""

# Standard imports
import argparse
import csv
import subprocess

# Manifests
MANIFESTS = {
    'CG001Qv4': 'CG001v4.0_Amplicon_Manifest_Panel4.0.3_20181101.tsv',
    'CG001Qv5': 'CG001v5.1_Amplicon_Manifest_Panel5.1.12_20200911.tsv'
}
MANIFEST_KEY_LG = len(list(MANIFESTS.keys())[0])

# AWS parameters
AWS_QUEUE = 'cchauve-orchestration-default'
AWS_DEF = 'cchauve'


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
            result.append((run_id, manifest))
    return result


if __name__ == "__main__":
    """
    Reads a list of runs_id,run_names in a CSV file, a branch for the
    indels-pipeline and an s3 bucket input directory and run the indels
    pipeline on AWS for all the runs in the list.
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

    runs_manifests_list = get_runs_manifests_list(args.runs_csv_file)
    for (run_id, manifest) in runs_manifests_list:
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
        cmd_options += ['\"--input_dir\"', f"\"s3://{args.s3_input}/\""]
        if args.s3_output is not None:
            cmd_options += ['\"--output_dir\"', f"\"s3://{args.s3_output}/\""]
        cmd_options += ['\"-resume\"']
        aws_cmd += [','.join(cmd_options)]
        aws_cmd += ['--region', 'ca-central-1']
        subprocess.call(aws_cmd)
