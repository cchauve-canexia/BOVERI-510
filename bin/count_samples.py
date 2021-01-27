#!/usr/bin/env python3
"""
Count number of samples of different types
"""

# Standard imports
import argparse

# Local imports
from common_utils import RUN_SAMPLES

# Default S3 directory containing results
CCHAUVE_S3_OUTPUT = 'cchauve-orchestration-ch'
# AWS cp command
AWS_CP = ['aws', 's3', 'cp']

if __name__ == "__main__":
    """
    Arguments:
    - input_log_file: input log file from a set of runs
    """
    # Input file
    ARGS_RUNS_FILE = ['input_log_file', None, 'Input log file']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: count samples')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    args = parser.parse_args()

    log_file = open(args.input_log_file, 'r').readlines()
    DNA_samples_nb, ctrl_samples_nb, misc_samples_nb = 0, 0, 0
    for log in log_file:
        log_split = log.rstrip().split('\t')
        log_header = log_split[0].split(':')
        log_type = log_header[0]
        if log_type == RUN_SAMPLES:
            samples_id_list = log_split[1].split()
            for sample_id in samples_id_list:
                test_dna = sample_id.lower().startswith('dna-')
                test_blank = sample_id.lower().startswith('blank')
                test_nf = sample_id.lower().startswith('nf')
                test_qmrs = sample_id.lower().startswith('qmrs')
                if test_dna:
                    DNA_samples_nb += 1
                elif test_blank or test_nf or test_qmrs:
                    ctrl_samples_nb += 1
                else:
                    misc_samples_nb += 1
    print(f"INFO\tpatient samples:\t{DNA_samples_nb}")
    print(f"INFO\tcontrol samples:\t{ctrl_samples_nb}")
    print(f"INFO\tmisc. samples:\t{misc_samples_nb}")
