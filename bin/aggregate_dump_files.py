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
from common_utils import (ALG_DUMP_HEADER, DUMP_FIELDS_SEP, VCF_DUMP_HEADER,
                          get_aggregated_alg_dump_file,
                          get_aggregated_vcf_dump_file, get_alg_dump_file,
                          get_vcf_dump_file)


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
    Aggregate all VCF/alignments dump files for a set of runs in (2x) six
    files:
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
    For alignments, these are the same files with VCF_DUMP_EXT replaced by
    ALG_DUMPEXT

    Arguments:
    - output_dir: directory where the results are read and written
    """
    def sort_data(in_dump):
        in_dump.sort(key=lambda x: x[0].replace('-CG001', ' '))
        in_dump.sort(key=itemgetter(3, 4))
        in_dump.sort(key=lambda x: (sort_chr(x[1]), int(x[2])))

    def split_data(dump_file, out_dict):
        for data_row in csv.DictReader(open(dump_file),
                                       delimiter=DUMP_FIELDS_SEP):
            sample_id = data_row['sample'].lower()
            if sample_id.startswith('dna-'):
                out_dict['DNA'].append(list(data_row.values()))
            elif sample_id.startswith('nf') or sample_id.startswith(
                    'blank') or sample_id.startswith('qmrs'):
                out_dict['ctrl'].append(list(data_row.values()))
            else:
                out_dict['misc'].append(list(data_row.values()))

    def dump_data(dump_file, data, header):
        with open(dump_file, 'w') as out_dump:
            writer = csv.writer(out_dump, delimiter=DUMP_FIELDS_SEP)
            writer.writerows([header] + data)

    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    args = parser.parse_args()

    # List of available runs
    run_id_list = []
    for x in os.listdir(args.output_dir):
        if os.path.isdir(os.path.join(args.output_dir, x)):
            run_id_list.append(x)

    # Extracting indels and alignments
    indels, alignments = {}, {}
    indels['DNA'], indels['ctrl'], indels['misc'] = [], [], []
    alignments['DNA'], alignments['ctrl'], alignments['misc'] = [], [], []
    for run_id in run_id_list:
        prefix = args.output_dir
        indels_dump_file = get_vcf_dump_file(run_id,
                                             prefix,
                                             INDELS,
                                             init=False)
        if os.path.isfile(indels_dump_file):
            split_data(indels_dump_file, indels)
        else:
            print(f"{indels_dump_file} missing")
        algs_dump_file = get_alg_dump_file(run_id, prefix, init=False)
        if os.path.isfile(algs_dump_file):
            split_data(algs_dump_file, alignments)
        else:
            print(f"{algs_dump_file} missing")
    # Aggregating indels
    for sample_type, indels_dump in indels.items():
        print(
            f"INFO\tindels calls in {sample_type} samples:\t{len(indels_dump)}"
        )
        sort_data(indels_dump)
        out_dump_file = get_aggregated_vcf_dump_file(f"{sample_type}_samples",
                                                     prefix,
                                                     INDELS,
                                                     init=False)
        dump_data(out_dump_file, indels_dump, VCF_DUMP_HEADER)
    # Aggregating alignments
    for sample_type, algs_dump in alignments.items():
        sort_data(algs_dump)
        out_dump_file = get_aggregated_alg_dump_file(f"{sample_type}_samples",
                                                     prefix,
                                                     init=False)
        dump_data(out_dump_file, algs_dump, ALG_DUMP_HEADER)
    # Grouping indels
    for sample_type, indels_dump in indels.items():
        aggregated_groups = []
        for variant, group in groupby(indels_dump,
                                      key=lambda x: (x[1], x[2], x[3], x[4])):
            aggregated_groups.append(aggregate_group(variant, group))
        aggregated_groups.sort(key=lambda x: x[0], reverse=True)
        out_dump_file = get_aggregated_vcf_dump_file(
            f"{sample_type}_grouped_samples", prefix, INDELS, init=False)
        nb_groups = len(aggregated_groups)
        print(f"INFO\tindels groups in {sample_type} samples:\t{nb_groups}")
        header = [
            'nb', 'chr', 'pos', 'ref', 'alt', 'avg_vaf', 'std_vaf', 'source',
            'features_cov', 'features_seq', 'annotation', 'sample:vaf'
        ]
        dump_data(out_dump_file, aggregated_groups, header)
