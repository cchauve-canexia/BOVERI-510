#!/usr/bin/env python3
""""
Aggregate all dump files for a run into a single dump file, sorted by sample_id
then position, then sequence
"""

import argparse
import csv
import os
from itertools import groupby
from operator import itemgetter

import numpy as np
from analysis_utils import INDELS
from common_utils import (VCF_DUMP_FIELDS_SEP, VCF_DUMP_VALUES_SEP,
                          get_aggregated_dump_file, get_vcf_dump_file)


def sort_chr(chrom):
    if chrom == 'chrX':
        return 23
    else:
        return int(chrom.replace('chr', ''))


def aggregate_group(variant, group):
    sample_list, vaf_list = [], []
    for v_sample in group:
        vaf = float(v_sample[5])
        source = v_sample[6]
        features_cov = v_sample[7]
        features_seq = v_sample[8]
        annotation = v_sample[9]
        sample_list.append(f"{v_sample[0]}:{round(vaf,4)}")
        vaf_list.append(vaf)
    avg_vaf = round(np.mean(vaf_list), 4)
    std_vaf = round(np.std(vaf_list), 4)
    out_group = [len(sample_list)] + list(variant) + [
        avg_vaf, std_vaf, source, features_cov, features_seq, annotation,
        ','.join(list(sample_list))
    ]
    return out_group


if __name__ == "__main__":
    """
    Aggregate all VCF dump files for a set of runs in six files:
    - a file DNA_samples_dump.tsv for indels calls from patient samples
      (starting with DNA-)
    - a file DNA_grouped_samples_indels_dump.tsv where indels have been grouped
      by the key (chr, position, reference, alternate)
    - a file ctrl_samples_dump.tsv indels calls from control samples
      (starting with nf, qmrs, blank)
    - a file ctrl_grouped_samples_indels_dump.tsv where indels have been
      grouped by the key (chr, position, reference, alternate)
    - a file misc_samples_dump.tsv indels calls from all other samples
    - a file misc_grouped_samples_indels_dump.tsv where indels have been
      grouped by the key (chr, position, reference, alternate)

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
                elif sample.startswith('nf') or sample.startswith(
                        'blank') or sample.startswith('qmrs'):
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
                                                 init=False)
        header = [
            'sample', 'chr', 'pos', 'ref', 'alt', 'VAF', 'source',
            'features_cov', 'features_seq', 'annotation'
        ]
        with open(out_dump_file, 'w') as out_dump:
            writer = csv.writer(out_dump, delimiter=VCF_DUMP_FIELDS_SEP)
            writer.writerows([header] + indels_dump)

    for sample_type, indels_dump in indels.items():
        aggregated_groups = []
        for variant, group in groupby(indels_dump,
                                      key=lambda x: (x[1], x[2], x[3], x[4])):
            aggregated_groups.append(aggregate_group(variant, group))
        aggregated_groups.sort(key=lambda x: x[0], reverse=True)
        out_dump_file = get_aggregated_dump_file(
            f"{sample_type}_grouped_samples", prefix, INDELS, init=False)
        nb_groups = len(aggregated_groups)
        print(f"INFO\tindels groups in {sample_type} samples:\t{nb_groups}")
        header = [
            'nb', 'chr', 'pos', 'ref', 'alt', 'avg_vaf', 'std_vaf', 'source',
            'features_cov', 'features_seq', 'annotation', 'sample:vaf'
        ]
        for aggregated_group in aggregated_groups:
            aggregated_group[8] = VCF_DUMP_VALUES_SEP.join(aggregated_group[8])
        with open(out_dump_file, 'w') as out_dump:
            writer = csv.writer(out_dump, delimiter=VCF_DUMP_FIELDS_SEP)
            writer.writerows([header] + aggregated_groups)
