#!/usr/bin/env python3
"""
Script to extract runs of co-located indels in  a set of runs results
"""

import argparse
import csv
import os
from collections import defaultdict

from common_utils import INFO


def read_output_log_file(log_file_path):
    """
    Reads an output log file to extract the run ID for runs that went OK.
    :param: log_file_path (str): path to output log file
    :return: list(str): lst of run IDs
    """
    log_file = open(log_file_path, 'r').readlines()
    run_id_list = []
    for log in log_file:
        log_split = log.rstrip().split('\t')
        log_header = log_split[0].split(':')
        log_type = log_header[0]
        if log_type == INFO:
            run_id = log_header[1]
            run_id_list.append(run_id)
    return run_id_list


def read_dump_file(dump_file_path, gap_len):
    """
    Reads a dump file and returns a list of groups of co-located indels where
    any two consecutive indel in a group are separated by at most gap_len
    bases.
    :param: dump_file_path (str): path to access the dump file of a run
    :param: gap_len (int): maximum number of bases between consecutive indels
    in a group

    :return: dict(str, list(str)): dictionary indexed by sample_id to a list of
    str, each representing a group of co-located indels
    """
    indels_data = defaultdict(list)
    colocated_indels = {}
    with open(dump_file_path) as dump_file:
        indels_reader = csv.DictReader(dump_file, delimiter='\t')
        for row in indels_reader:
            indel = (row['chr'], int(row['pos']), row['ref'], row['alt'])
            indels_data[row['sample']].append(indel)
    for sample_id, sample_indels in indels_data.items():
        sample_indels.sort(key=lambda x: (x[0], x[1]))
        prev_chr, prev_pos, current_list, colocated_indels_list = '', 0, [], []
        for indel in sample_indels:
            current_chr, current_pos = indel[0], indel[1]
            if current_chr == prev_chr and current_pos - prev_pos <= gap_len:
                current_list.append('.'.join([str(x) for x in indel]))
            else:
                if len(current_list) > 1:
                    colocated_indels_list.append(current_list.copy())
                current_list = ['.'.join([str(x) for x in indel])]
            prev_chr, prev_pos = current_chr, current_pos
        if len(current_list) > 1:
            colocated_indels_list.append(current_list.copy())
        colocated_indels[sample_id] = [
            '___'.join(g) for g in colocated_indels_list
        ]
    return colocated_indels


if __name__ == "__main__":
    """
    Reads the output log file for a set of runs, reads the dump file for each
    successful run and detects groups of co-located indels.
    Generates a text file containing each such group and the samples where it
    was detected.

    Arguments:
    - output_log_file: path to the output log file for a set of runs
    - output_dir: path to the directory containing the output of the runs
    - output_file: path to the file that contains the groups of co-located
      indels
    - gap_len (optional): integer defining ghe maximum gap between consecutive
      indels to put them into the same group; default = 5
    """
    # Input file
    ARGS_INPUT_FILE = ['output_log_file', None, 'Output log file']
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Runs output directory']
    # Results directory
    ARGS_OUTPUT_FILE = ['output_file', None, 'Output file']
    # Results directory
    ARGS_GAP_LEN = ['-g', '--gap_len', 'Gap length']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: detection of groups of co-located indels'
    )
    parser.add_argument(ARGS_INPUT_FILE[0], type=str, help=ARGS_INPUT_FILE[2])
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    parser.add_argument(ARGS_OUTPUT_FILE[0],
                        type=str,
                        help=ARGS_OUTPUT_FILE[2])
    parser.add_argument(ARGS_GAP_LEN[0],
                        ARGS_GAP_LEN[1],
                        type=int,
                        default=5,
                        help=ARGS_GAP_LEN[2])
    args = parser.parse_args()

    run_id_list = read_output_log_file(args.output_log_file)
    indel_groups_to_sample = defaultdict(list)
    nb_runs, nb_samples, nb_group_occurrences, samples_with_group = 0, 0, 0, []
    for run_id in run_id_list:
        nb_runs += 1
        dump_file_name = f"{run_id}_indels_dump.tsv"
        dump_file_path = os.path.join(args.output_dir, run_id, dump_file_name)
        colocated_indels = read_dump_file(dump_file_path, args.gap_len)
        for sample_id, indel_groups_list in colocated_indels.items():
            nb_samples += 1
            if len(indel_groups_list) > 0:
                for indel_group in indel_groups_list:
                    indel_groups_to_sample[indel_group].append(
                        f"{run_id}.{sample_id}")
                    nb_group_occurrences += 1
                    if (run_id, sample_id) not in samples_with_group:
                        samples_with_group.append((run_id, sample_id))
    indel_groups_list = list(indel_groups_to_sample.keys())
    indel_groups_list.sort()
    output_file = open(args.output_file, 'w')
    output_file.write(
        f"#nb_runs:<{nb_runs}>\tnb_samples_with_indels:<{nb_samples}>\n")
    output_file.write(f"#nb_indel_groups:<{len(indel_groups_list)}>\n")
    output_file.write(
        f"#nb_indels_group_occurrences:<{nb_group_occurrences}>\n")
    output_file.write(
        f"#nb_samples_with_indels_group:<{len(samples_with_group)}>\n")
    output_file.write('#colocated_indels_group\tnumber_of_occuring_samples\n')
    output_file.write('#list_of_(run_id.sample_id)')
    indel_group_id = 1
    for indel_group in indel_groups_list:
        sample_id_list = indel_groups_to_sample[indel_group]
        line_1 = f"\n>{indel_group_id}\t{len(sample_id_list):5}\t{indel_group}"
        line_2 = f"\n{' '.join(sample_id_list)}"
        output_file.write(line_1)
        output_file.write(line_2)
        indel_group_id += 1
