#!/usr/bin/env python3
"""
Extract the csv entries for a list of failed runs
"""

import argparse
import csv

if __name__ == "__main__":
    # Input CSV file of all launched runs
    ARGS_RUNS_FILE = ['runs_csv_file', None, 'Runs CSV file']
    # Input test file of failed runs
    ARGS_FAILED_RUNS_FILE = [
        'failed_runs_txt_file', None, 'Failed runs text file'
    ]
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    parser.add_argument(ARGS_FAILED_RUNS_FILE[0],
                        type=str,
                        help=ARGS_FAILED_RUNS_FILE[2])
    args = parser.parse_args()

    # Failed runs
    failed_runs = [
        x.rstrip() for x in open(args.failed_runs_txt_file).readlines()
    ]
    # Reading list of all runs and extracting failed ones
    retry_csv_file_name = args.failed_runs_txt_file.replace(
        '.txt', '_retry.csv')
    retry_csv_file = open(retry_csv_file_name, 'w')
    retry_csv_file.write('Run, ID')
    with open(args.runs_csv_file) as csvfile:
        runs_data = csv.reader(csvfile, delimiter=',')
        for row in runs_data:
            run_id = row[1]
            if run_id in failed_runs:
                retry_csv_file.write(f"\n{row[0]},{row[1]}")
    retry_csv_file.close()
