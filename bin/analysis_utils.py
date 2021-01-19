#!/usr/bin/env python3
"""
Functions to analyze the results of a run of the indels pipeline on AWS
"""

# Standard imports
import argparse
import csv
import os
import subprocess
import tarfile

# Third-party imports
import boto3
# mMre user friendly version of some parts of boto3
# Useful for opening files
# Defaults to regular open when it can
import vcf
from smart_open import open

# Directory containing results
S3_OUTPUT = 'cchauve-orchestration-ch'

# Suffixes of files generated by the pipeline
INDELS_FILE_SUFFIX = '_indels_filtered_snpeff.vcf'
INDELS_FILE_SUFFIX_TGZ = f"{INDELS_FILE_SUFFIX}.tar.gz"
SNPS_FILE_SUFFIX = '_snps_filtered_snpeff.vcf'
SNPS_FILE_SUFFIX_TGZ = f"{SNPS_FILE_SUFFIX}.tar.gz"
FILTERS_LOG_FILE_SUFFIX = '_samples_filters.log'
MAIN_FILE_SUFFIX = '_main.tar.gz'
MAIN_LOG_FILE_SUFFIX = '_main.log'
FASTQ_FILES_SUFFIX = '_annotated.fq.tar.gz'

# Keys to differentiate indels and SNPs
INDELS, SNPS = 'indels', 'snps'
CALLS_FILE_SUFFIX_TGZ = {
    INDELS: INDELS_FILE_SUFFIX_TGZ,
    SNPS: SNPS_FILE_SUFFIX_TGZ
}
CALLS_FILE_SUFFIX = {INDELS: INDELS_FILE_SUFFIX, SNPS: SNPS_FILE_SUFFIX}

# AWS cp command
AWS_CP = ['aws', 's3', 'cp']

# VCF and VCF archive files extensions
VCF_DUMP_EXT = '_dump.tsv'
VCF_DUMP_HEADER = [
    'sample', 'chr', 'pos', 'ref', 'alt', 'VAF', 'source_coverage',
    'total_coverage', 'max_coverage', 'source'
]
# Dump files separators
VCF_DUMP_FIELDS_SEP = '\t'
VCF_DUMP_VALUES_SEP = ','
# Variant features to print: taken from indesl-pipeline/bin/feature_utils.py
SOURCE_COV = 'SCOV'
TOTAL_COV = 'TCOV'
MAX_COV = 'MCOV'
SOURCE = 'AMPLICONS'

# Log steps
WARNINGS_OUTPUT_SUFFIX = {
    'bin.reads_utils': '_reads.txt',
    'bin.clusters_utils': '_clusters.txt',
    'bin.alignments_utils': '_alignments.txt',
    'bin.variants_utils': '_variants.txt',
    'bin.variants_graph_utils': '_variants_graph.txt'
}


def get_files_in_s3(run_id):
    """
    Get a list of files from the indels pipeline output of a run.
    :param: run_id (str): 201014_M03829_0366_000000000-JBV6Y

    :return: list(str): file paths of files for the output of run run_id
    """
    s3_client = boto3.client('s3')
    files = [
        obj['Key']
        for obj in s3_client.list_objects_v2(Bucket=S3_OUTPUT,
                                             Prefix=run_id + '/')['Contents']
    ]
    return files


def get_runs_list(runs_csv_file):
    """
    Get from a CSV file containing runs ID and runs name a list run IDs
    :param: runs_csv_file (str): path to the input CSV file
    :assumption: column 0 is run name, column 1 is run ID

    :return: list(str): list of runs ID
    """
    result = []
    with open(runs_csv_file) as csvfile:
        runs_data = csv.reader(csvfile, delimiter=',')
        for row in runs_data:
            run_id = row[1]
            result.append(run_id)
    return result


def get_samples_list(run_id):
    """
    Returns the list of samples for run run_id
    :params: run_id (str): ID of the run

    :return: list(str): list of sample IDs
    """
    files = get_files_in_s3(run_id)
    samples_list = []
    for f in files:
        if f.endswith(MAIN_FILE_SUFFIX):
            _, main_file_tgz = os.path.split(f)
            samples_list.append(main_file_tgz.replace(MAIN_FILE_SUFFIX, ''))
    return samples_list


def init_dump_vcf_file(dump_file):
    """
    Create a dump_file with the dump header
    :param: dump_file (str): path to the dump file
    """
    out_dump = open(dump_file, 'w')
    out_dump.write(VCF_DUMP_FIELDS_SEP.join(VCF_DUMP_HEADER))
    out_dump.close()


def dump_vcf_file(sample_id, in_file, out_file, append=True):
    """
    Dump a VCF file for a sample into a TSV file
    :param: sample_id (str): sample ID
    :param: in_file (str): path to input VCF file
    :param: out_file (str): path to output TSV file
    :param: append (bool): if True, dump is appended otherwise new file created
    """
    if not append:
        out_file = open(out_file, 'w')
        out_file.write(VCF_DUMP_FIELDS_SEP.join(VCF_DUMP_HEADER))
    else:
        out_file = open(out_file, 'a')
    sample_vcf_reader = vcf.Reader(open(in_file, 'r'))
    for record in sample_vcf_reader:
        record_str = [record.CHROM, record.POS, record.REF, record.ALT[0]]
        info = record.INFO
        source_str = VCF_DUMP_VALUES_SEP.join(info[SOURCE])
        info_str = [
            info['VAF'], info[SOURCE_COV], info[TOTAL_COV], info[MAX_COV],
            source_str
        ]
        out_str = [sample_id] + record_str + info_str
        out_file.write('\n')
        out_file.write(VCF_DUMP_FIELDS_SEP.join([str(x) for x in out_str]))
    out_file.close()


def out_dir(run_id, prefix):
    """
    Returns the path to the output dir for a run
    :param: run_id (str) run ID
    :param: prefix (str): prefix of the path

    :return: str: output directory path
    """
    return os.path.join(prefix, run_id)


def sample_vcf_file(run_id, sample_id, prefix, v_type):
    """
    Returns the path to a VCF file for a given sample of a run
    :param: run_id (str): run ID
    :param: sample_id (str): sample ID
    :param: prefix (str): prefix of the path to output directory
    :param: v_type (str): SNPS or INDELS

    :return: str: path to VCF file
    """
    return os.path.join(prefix, run_id,
                        f"{sample_id}{CALLS_FILE_SUFFIX[v_type]}")


def dump_file(run_id, prefix, v_type, init=True):
    """
    Returns the path to a variant dump file for a run
    :param: run_id (str): run ID
    :param: prefix (str): prefix of the path to output directory
    :param: v_type (str): SNPS or INDELS

    :return: str: path to dump file
    """
    dump_file = os.path.join(prefix, run_id,
                             f"{run_id}_{v_type}{VCF_DUMP_EXT}")
    if init:
        init_dump_vcf_file(dump_file)
    return dump_file


def extract_vcf_files(run_id,
                      v_type=INDELS,
                      prefix='.',
                      to_dump=False,
                      to_keep=True):
    """
    Reads indels files of run run_id
    :param: run_id (str): ID of the run
    :param: v_type (str): SNPS or INDELS
    :param: prefix (str): prefix of the output directory
    :param: to_dump (bool): if True a dump file is created
    """
    files = get_files_in_s3(run_id)
    for f in files:
        if f.endswith(CALLS_FILE_SUFFIX_TGZ[v_type]):
            subprocess.call(AWS_CP + [f"s3://{S3_OUTPUT}/{f}", '.'])
            _, vcf_file_tgz = os.path.split(f)
            tarfile.open(vcf_file_tgz,
                         'r:gz').extractall(path=out_dir(run_id, prefix))
            if to_dump:
                out_dump_file = dump_file(run_id, prefix, v_type, init=True)
                for sample_id in get_samples_list(run_id):
                    in_vcf = sample_vcf_file(run_id, sample_id, prefix, v_type)
                    dump_vcf_file(sample_id,
                                  in_vcf,
                                  out_dump_file,
                                  append=True)
                    if not to_keep:
                        os.remove(in_vcf)
            os.remove(vcf_file_tgz)


def extract_main_warnings(run_id, prefix='.'):
    """
    Reads main_log files of run run_id
    :param: run_id (str): ID of the run
    :param: prefix (str): prefix of the output directory
    """
    files = get_files_in_s3(run_id)
    for warning_suffix in WARNINGS_OUTPUT_SUFFIX.values():
        warning_out_path = os.path.join(out_dir(run_id, prefix),
                                        f"{run_id}_warnings{warning_suffix}")
        warning_out = open(warning_out_path, 'w')
        warning_out.close()
    warning_out_file = {}
    for f in files:
        if f.endswith(MAIN_LOG_FILE_SUFFIX):
            for warning_key, warning_suffix in WARNINGS_OUTPUT_SUFFIX.items():
                warning_out_path = os.path.join(
                    out_dir(run_id, prefix),
                    f"{run_id}_warnings{warning_suffix}")
                warning_out_file[warning_key] = open(warning_out_path, 'a')
            s3_file_path = os.path.join('s3://', S3_OUTPUT, f)
            log_file = open(s3_file_path, 'r').readlines()
            for line in log_file:
                line_split = line.rstrip().split('\t')
                if line_split[1] == '[WARNING]':
                    step, sample_amplicon = line_split[2].split()
                    msg = line_split[3]
                    if sample_amplicon[0:5].lower() != 'blank':
                        warning_out_file[step].write(
                            f"{sample_amplicon}\t{msg}\n")
            for warning_key, warning_out in warning_out_file.items():
                warning_out.close()


if __name__ == "__main__":
    """
    Reads a list of runs_id,run_names in a CSV file, and creates summaries of
    calls and warnings
    """
    # Input file
    ARGS_RUNS_FILE = ['runs_csv_file', None, 'Runs CSV file']
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    args = parser.parse_args()

    runs_list = get_runs_list(args.runs_csv_file)
    for run_id in runs_list:
        os.makedirs(out_dir(run_id, args.output_dir), exist_ok=True)
        # Extracting indels calls
        # extract_vcf_files(run_id,
        #                   v_type=INDELS,
        #                   prefix=args.output_dir,
        #                   to_dump=True,
        #                   to_keep=False)
        # Extracting warnings
        extract_main_warnings(run_id, prefix=args.output_dir)
