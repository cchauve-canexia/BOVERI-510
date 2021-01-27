#!/usr/bin/env python3
"""
Retrieve results from a run
"""

# Standard imports
import argparse
import os
import subprocess
import tarfile

from common_utils import get_files_in_s3

# Default S3 directory containing results
CCHAUVE_S3_OUTPUT = 'cchauve-orchestration-ch'
# AWS cp command
AWS_CP = ['aws', 's3', 'cp']

if __name__ == "__main__":
    """
    Retrieve the main and VCF files from a run
    Arguments:
    - run_id: run ID
    - output_dir: directory where the results are written
    - s3_bucket (optional, default cchauve-orchestration-ch): bucket where to
      fetch indels pipeline output files.
    """
    # Run ID
    ARGS_RUN_ID = ['run_id', None, 'Run ID']
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    # S3 bucket containing the reuslts
    ARGS_S3_BUCKET = ['-s3', '--s3_bucket', 'S3 bucket containing the results']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_RUN_ID[0], type=str, help=ARGS_RUN_ID[2])
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    parser.add_argument(ARGS_S3_BUCKET[0],
                        ARGS_S3_BUCKET[1],
                        default=CCHAUVE_S3_OUTPUT,
                        type=str,
                        help=ARGS_S3_BUCKET[2])
    args = parser.parse_args()

    out_dir = os.path.join(args.output_dir, args.run_id)
    os.makedirs(out_dir, exist_ok=True)
    s3_files = get_files_in_s3(args.run_id, args.s3_bucket)
    for file_path in s3_files:
        if file_path.endswith('_main.tar.gz') or file_path.endswith(
                '_vcf.tar.gz'):
            subprocess.call(
                AWS_CP +
                [os.path.join('s3://', args.s3_bucket, file_path), out_dir])
            tarfile.open(os.path.join(args.output_dir, file_path),
                         'r:gz').extractall(path=out_dir)
        elif file_path.endswith('.yaml'):
            subprocess.call(
                AWS_CP +
                [os.path.join('s3://', args.s3_bucket, file_path), out_dir])
