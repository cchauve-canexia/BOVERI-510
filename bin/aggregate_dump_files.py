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
                          get_vcf_dump_file)


def sort_chr(chrom):
    if chrom == 'chrX':
        return 23
    else:
        return int(chrom.replace('chr', ''))


if __name__ == "__main__":
    """
    Aggregate all VCF dump files for a set of runs in three files:
    - a file DNA_samples_dump.tsv for indels calls from patient samples
      (starting with DNA-)
    - a file control_samples_dump.tsv indels calls from control samples
      (starting with nf-, qmrs-, blank-)
    - a file misc_samples_dump.tsv indels calls from all other samples
    Arguments:
    - output_dir: directory where the results are read and written

    """
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    args = parser.parse_args()

    run_id_list = []
    for x in os.listdir(args.output_dir):
        if os.path.isdir(os.path.join(args.output_dir, x)):
            run_id_list.append(x)

    indels = {}
    indels['DNA'], indels['ctrl'], indels['misc'] = [], [], []
    for run_id in run_id_list:
        prefix = args.output_dir
        in_dump_file = get_vcf_dump_file(run_id, prefix, INDELS, init=False)
        if os.path.isfile(in_dump_file):
            for data_row in csv.reader(open(in_dump_file),
                                       delimiter=VCF_DUMP_FIELDS_SEP):
                sample = data_row[0].lower()
                if sample.startswith('dna-'):
                    indels['DNA'].append(data_row)
                elif sample.startswith('nf-') or sample.startswith(
                        'blank-') or sample.startswith('qmrs-'):
                    indels['ctrl'].append(data_row)
                elif sample != 'sample':
                    indels['misc'].append(data_row)
        else:
            print(f"{in_dump_file} missing")
    for sample_type, indels_dump in indels.items():
        print(
            f"INFO\tindels calls in {sample_type} samples:\t{len(indels_dump)}"
        )
        indels_dump.sort(key=lambda x: float(x[5]))
        indels_dump.sort(key=lambda x: x[0].replace('-CG001', ' '))
        indels_dump.sort(key=itemgetter(3, 4))
        indels_dump.sort(key=lambda x: (sort_chr(x[1]), int(x[2])))
        out_dump_file = get_aggregated_dump_file(f"{sample_type}_samples",
                                                 prefix,
                                                 INDELS,
                                                 init=True)
        with open(out_dump_file, 'a') as out_dump:
            writer = csv.writer(out_dump, delimiter=VCF_DUMP_FIELDS_SEP)
            writer.writerows(indels_dump)
