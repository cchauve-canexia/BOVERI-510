#!/usr/bin/env python3
""""
Aggregate all dump files for a run into a single dump file, sorted by sample_id
then position, then sequence
"""

import argparse
import csv
import os
from operator import itemgetter

from analysis_utils import INDELS
from common_utils import (VCF_DUMP_FIELDS_SEP, get_aggregated_dump_file,
                          get_vcf_dump_file, read_input_log_file)


def sort_chr(chrom):
    if chrom == 'chrX':
        return 23
    else:
        return int(chrom.replace('chr', ''))


if __name__ == "__main__":
    """
        TODO
    """
    # Input file
    ARGS_RUNS_FILE = ['input_log_file', None, 'Input log file']
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    args = parser.parse_args()

    (sample_id_lists,
     unprocessed_runs) = read_input_log_file(args.input_log_file)

    indels = []
    for (run_id, run_name), sample_id_list in sample_id_lists.items():
        prefix = args.output_dir
        in_dump_file = get_vcf_dump_file(run_id, prefix, INDELS, init=False)
        if os.path.isfile(in_dump_file):
            for data_row in csv.reader(open(in_dump_file),
                                       delimiter=VCF_DUMP_FIELDS_SEP):
                if data_row[0] != 'sample':
                    indels.append(data_row)
        else:
            print(f"{in_dump_file} missing")
    indels.sort(key=lambda x: float(x[5]))
    indels.sort(key=lambda x: x[0].replace('-CG001', ' '))
    indels.sort(key=itemgetter(3, 4))
    indels.sort(key=lambda x: (sort_chr(x[1]), int(x[2])))

    out_dump_file = get_aggregated_dump_file('all_samples',
                                             prefix,
                                             INDELS,
                                             init=True)
    with open(out_dump_file, 'a') as out_dump:
        writer = csv.writer(out_dump, delimiter=VCF_DUMP_FIELDS_SEP)
        writer.writerows(indels)
