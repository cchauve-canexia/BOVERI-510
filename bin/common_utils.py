"""
Constants and functions common to several modules
"""

import os

import boto3

ERROR_RUN_NO_DATA = 'no data'
ERROR_RUN_NO_SAMPLE = 'no sample'
ERROR_RUN_NO_CORRECT_SAMPLE = 'no correct sample'
ERROR_RUN_UNPROCESSED = 'unprocessed'
ERROR_FASTQ = 'error FASTQ files'
ERROR_NONE = 'OK'
WARNING = 'WARNING'
INFO = 'INFO'
RUN_ID = 'RUN.ID'
AWS_CMD = 'AWS'
RUN_SAMPLES = 'RUN.SAMPLES'

# Dump files separators
VCF_DUMP_FIELDS_SEP = '\t'
VCF_DUMP_VALUES_SEP = ','
VCF_DUMP_EXT = '_dump.tsv'
VCF_DUMP_HEADER = [
    'sample', 'chr', 'pos', 'ref', 'alt', 'VAF', 'source_coverage',
    'total_coverage', 'max_coverage', 'source', 'annotation'
]


def get_files_in_s3(prefix, s3_bucket):
    """
    Get a list of files from the indels pipeline output of a run.
    :param: prefix (str): path to run data, e.g.
    input/201014_M03829_0366_000000000-JBV6Y
    :param: s3_bucket (str): S3 bucket containing files to read,
    e.g. 'cchauve-orchestration-ch', 'ch-testdata'

    :return: list(str): file paths of files in directory prefix;
    None if the directory is empty or does not exist
    """
    s3_client = boto3.client('s3')
    s3_objects = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    if s3_objects['KeyCount'] == 0:
        return None
    else:
        return [obj['Key'] for obj in s3_objects['Contents']]


def init_vcf_dump_file(dump_file):
    """
    Create a dump_file with the dump header
    :param: dump_file (str): path to the dump file
    """
    out_dump = open(dump_file, 'w')
    out_dump.write(VCF_DUMP_FIELDS_SEP.join(VCF_DUMP_HEADER))
    out_dump.close()


def get_vcf_dump_file(run_id, prefix, v_type, init=True):
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
        init_vcf_dump_file(dump_file)
    return dump_file


def get_aggregated_dump_file(file_name, prefix, v_type, init=True):
    """
    Returns the path to a variant dump file for all runs
    :param: file_name (str): file name prefix
    :param: prefix (str): prefix of the path to output directory
    :param: v_type (str): SNPS or INDELS

    :return: str: path to dump file
    """
    dump_file = os.path.join(prefix, f"{file_name}_{v_type}{VCF_DUMP_EXT}")
    if init:
        init_vcf_dump_file(dump_file)
    return dump_file


def read_input_log_file(log_file_path):
    """
    Reads an input log file to extract the run IDs and the sample IDs  list for
    each run.
    Writes a CSV file
    :param: log_file_path (str): path to input log file
    :return: dict(str,list(str)), list((str,str)):
    dictionary, indexed by pairs (run_id, run_name), of sample lists
    list (run_id, run_name) of unprocessed runs
    """
    log_file = open(log_file_path, 'r').readlines()
    unprocessed_runs, sample_id_lists, run_names = [], {}, {}
    for log in log_file:
        log_split = log.rstrip().split('\t')
        log_header = log_split[0].split(':')
        log_type = log_header[0]
        if log_type == RUN_ID:
            (run_id, run_name) = log_header[1].split('.')
            run_names[run_id] = run_name
        elif log_type == WARNING and log_split[1] == ERROR_RUN_UNPROCESSED:
            run_id = log_header[1]
            unprocessed_runs.append((run_id, run_names[run_id]))
        elif log_type == RUN_SAMPLES:
            run_id = log_header[1]
            sample_id_lists[(run_id, run_names[run_id])] = log_split[1].split()
    return (sample_id_lists, unprocessed_runs)
