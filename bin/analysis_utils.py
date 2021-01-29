#!/usr/bin/env python3
"""
Functions to analyze the results of a run of the indels pipeline on AWS
"""

# Standard imports
import argparse
import os
import subprocess
import tarfile

# Third-party imports
import vcf
# Local imports
from common_utils import (ERROR_NONE, INFO, VCF_DUMP_FIELDS_SEP,
                          VCF_DUMP_HEADER, VCF_DUMP_VALUES_SEP, WARNING,
                          get_files_in_s3, get_vcf_dump_file,
                          read_input_log_file)
from smart_open import open

# Default S3 directory containing results
CCHAUVE_S3_OUTPUT = 'cchauve-orchestration-ch'

# Suffixes of files generated by the pipeline
INDELS_FILE_SUFFIX = '_indels_filtered_snpeff.vcf'
INDELS_FILE_SUFFIX_TGZ = f"{INDELS_FILE_SUFFIX}.tar.gz"
SNPS_FILE_SUFFIX = '_snps_filtered_snpeff.vcf'
SNPS_FILE_SUFFIX_TGZ = f"{SNPS_FILE_SUFFIX}.tar.gz"
FILTERS_LOG_FILE_SUFFIX = '_samples_filters.log'
MAIN_FILE_SUFFIX = '_main.tar.gz'
MAIN_LOG_FILE_SUFFIX = '_main.log'
PRE_LOG_FILE_SUFFIX = '_preprocessing.log'
FASTQ_FILES_SUFFIX = '_L001_annotated.fq.tar.gz'

# Keys to differentiate indels and SNPs
INDELS, SNPS = 'indels', 'snps'
CALLS_FILE_SUFFIX_TGZ = {
    INDELS: INDELS_FILE_SUFFIX_TGZ,
    SNPS: SNPS_FILE_SUFFIX_TGZ
}
CALLS_FILE_SUFFIX = {INDELS: INDELS_FILE_SUFFIX, SNPS: SNPS_FILE_SUFFIX}

# AWS cp command
AWS_CP = ['aws', 's3', 'cp']

# Variant features to print: taken from indesl-pipeline/bin/feature_utils.py
# Variant features to print: taken from indesl-pipeline/bin/feature_utils.py
SOURCE_COV = 'SCOV'
TOTAL_COV = 'TCOV'
MAX_COV = 'MCOV'
SOURCE = 'AMPLICONS'
WT_RU = 'WRU1'
WT_RU_CNB = 'WRU2'
WT_RU_LEFT_CNB = 'WRU3'
WT_RU_RIGHT_CNB = 'WRU4'
V_RU = 'VRU1'
V_RU_CNB = 'VRU2'
V_RU_LEFT_CNB = 'VRU3'
V_RU_RIGHT_CNB = 'VRU4'
HP_LEFT_LEN = 'HPL1'
HP_LEFT_BASE = 'HPL2'
HP_RIGHT_LEN = 'HPR1'
HP_RIGHT_BASE = 'HPR2'

FEATURES_COV = [SOURCE_COV, TOTAL_COV, MAX_COV]
FEATURES_SEQ = [
    WT_RU, WT_RU_CNB, WT_RU_LEFT_CNB, WT_RU_RIGHT_CNB, V_RU, V_RU_CNB,
    V_RU_LEFT_CNB, V_RU_RIGHT_CNB, HP_LEFT_LEN, HP_LEFT_BASE, HP_RIGHT_LEN,
    HP_RIGHT_BASE
]

# Log steps
WARNINGS_OUTPUT_SUFFIX = {
    'bin.reads_utils': '_reads.tsv',
    'bin.clusters_utils': '_clusters.tsv',
    'bin.alignments_utils': '_alignments.tsv',
    'bin.variants_utils': '_variants.tsv',
    'bin.variants_graph_utils': '_variants_graph.tsv'
}


def out_dir(run_id, prefix):
    """
    Returns the path to the output directory for a run
    :param: run_id (str) run ID
    :param: prefix (str): prefix of the path

    :return: str: output directory path
    """
    return os.path.join(prefix, run_id)


# Checks all expected output files are in the output directory


def check_output_files(run_id, sample_id_list, s3_files):
    """
    Checks that the output directory for run_id has all the expected files:
    - a run parameters YAML file <run_id>.yaml
    - a filters log file <run_id>_samples_filters.log
    - an indels VCF archive <run_id>_indels_filtered_snpeff.vcf.tar.gz
    - a SNPs VCF archive <run_id>_snpss_filtered_snpeff.vcf.tar.gz
    - for each sample sample_id:
        - a preprocessing log file for each sample:
          <run_id>_<sample_id>_preprocessing.log
        - a main log file file for each sample:
          <run_id>_<sample_id>_main.log
        - an annotated FASTQ files archive <sample_id>_annotated.fq.tar.gz
        - a main output archive <sample_id>_main.tar.gz

    :param: run_id (str): run ID
    :param: sample_id_list (list(str)): list of sample ID
    :param: s3_files (list(str)): list of s3 files for run_id

    :return: bool: True if all expected files are present and not other one.
    """
    # Extracting S3 file names
    s3_file_names = []
    for file_path in s3_files:
        _, file_name = os.path.split(file_path)
        if file_name != '':
            s3_file_names.append(file_name)
    # Building the list of expected files
    expected_file_names = []
    expected_file_names.append(f"{run_id}.yaml")
    expected_file_names.append(f"{run_id}{INDELS_FILE_SUFFIX_TGZ}")
    expected_file_names.append(f"{run_id}{SNPS_FILE_SUFFIX_TGZ}")
    expected_file_names.append(f"{run_id}{FILTERS_LOG_FILE_SUFFIX}")
    for sample_id in sample_id_list:
        expected_file_names.append(
            f"{run_id}_{sample_id}{PRE_LOG_FILE_SUFFIX}")
        expected_file_names.append(
            f"{run_id}_{sample_id}{MAIN_LOG_FILE_SUFFIX}")
        expected_file_names.append(f"{sample_id}{FASTQ_FILES_SUFFIX}")
        expected_file_names.append(f"{sample_id}{MAIN_FILE_SUFFIX}")
    return set(s3_file_names) == set(expected_file_names)


# VCF files dumping


def dump_sample_vcf_file(run_id,
                         sample_id,
                         in_file,
                         out_file,
                         log_file,
                         append=True):
    """
    Dump a VCF file for a sample into a TSV file
    :param: run_id (str): run ID
    :param: sample_id (str): sample ID
    :param: in_file (str): path to input VCF file
    :param: out_file (str): path to output TSV file
    :param: log_file (opened file): log file
    :param: append (bool): if True, dump is appended otherwise new file created
    """
    if not append:
        out_file = open(out_file, 'w')
        out_file.write(VCF_DUMP_FIELDS_SEP.join(VCF_DUMP_HEADER))
    else:
        out_file = open(out_file, 'a')
    if os.stat(in_file).st_size == 0:
        log_file.write(
            f"{WARNING}:{run_id}.{sample_id}\t{in_file} empty VCF file\n")
    else:
        sample_vcf_reader = vcf.Reader(open(in_file, 'r'))
        for record in sample_vcf_reader:
            record_str = [record.CHROM, record.POS, record.REF, record.ALT[0]]
            v_info = record.INFO
            features_cov = [
                f"{feature}:{v_info[feature]}" for feature in FEATURES_COV
            ]
            features_seq = [
                f"{feature}:{v_info[feature]}" for feature in FEATURES_SEQ
            ]
            source = VCF_DUMP_VALUES_SEP.join(v_info[SOURCE])
            annotation = VCF_DUMP_VALUES_SEP.join(v_info['ANN'])
            v_info_str = [
                v_info['VAF'], source,
                VCF_DUMP_VALUES_SEP.join(features_cov),
                VCF_DUMP_VALUES_SEP.join(features_seq), annotation
            ]
            out_str = [sample_id] + record_str + v_info_str
            out_file.write('\n')
            out_file.write(VCF_DUMP_FIELDS_SEP.join([str(x) for x in out_str]))
    out_file.close()


def get_sample_vcf_file(run_id, sample_id, prefix, v_type):
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


def extract_vcf_files(run_id,
                      sample_id_list,
                      s3_bucket,
                      log_file,
                      v_type=INDELS,
                      prefix='.',
                      to_dump=False,
                      to_keep=True):
    """
    Reads and optionally dump indels VCF files of run run_id.
    :param: run_id (str): ID of the run
    :param: sample_id_list (list(str)): list of sample ID
    :param: s3_bucket (str): s3 bucket where to fetch the results
    :param: log_file (opened file): log file
    :param: v_type (str): SNPS or INDELS
    :param: prefix (str): prefix of the output directory
    :param: to_dump (bool): if True a dump file is created
    :param: to_keep (bool): if True VCF file from the archive are not deleted
    """
    vcf_file_name = f"{run_id}{CALLS_FILE_SUFFIX_TGZ[v_type]}"
    vcf_file_path = os.path.join('s3://', s3_bucket, run_id, vcf_file_name)
    subprocess.call(AWS_CP + [vcf_file_path, '.'])
    tarfile.open(vcf_file_name,
                 'r:gz').extractall(path=out_dir(run_id, prefix))
    if to_dump:
        out_dump_file = get_vcf_dump_file(run_id, prefix, v_type, init=True)
        for sample_id in sample_id_list:
            in_vcf = get_sample_vcf_file(run_id, sample_id, prefix, v_type)
            dump_sample_vcf_file(run_id,
                                 sample_id,
                                 in_vcf,
                                 out_dump_file,
                                 log_file,
                                 append=True)
            if not to_keep:
                os.remove(in_vcf)
    os.remove(vcf_file_name)


# Analysis of log files


def check_log_files(run_id, sample_id_list, s3_bucket):
    """
    Checks that all log files are complete
    :param: run_id (str): run ID
    :param: sample_i_list (list(str): list of samples ID
    :param: s3_bucket (str): S3 bucket containing the output results

    :return: bool: True if all log files are complete
    """
    def get_last_line(file):
        return open(file).readlines()[-1].rstrip()

    filters_log_name = f"{run_id}{FILTERS_LOG_FILE_SUFFIX}"
    filters_log_path = os.path.join('s3://', s3_bucket, run_id,
                                    filters_log_name)
    filters_last_line = get_last_line(filters_log_path)
    if not ('FILTERS' in filters_last_line and 'total' in filters_last_line):
        return False
    for sample_id in sample_id_list:
        pre_log_name = f"{run_id}_{sample_id}{PRE_LOG_FILE_SUFFIX}"
        pre_log_path = os.path.join('s3://', s3_bucket, run_id, pre_log_name)
        pre_last_line = get_last_line(pre_log_path)
        if not ('PREPROCESSING' in pre_last_line and 'total' in pre_last_line):
            return False
        main_log_name = f"{run_id}_{sample_id}{MAIN_LOG_FILE_SUFFIX}"
        main_log_path = os.path.join('s3://', s3_bucket, run_id, main_log_name)
        main_last_line = get_last_line(main_log_path)
        if not ('PIPELINE' in main_last_line
                and 'total_time' in main_last_line):
            return False
    return True


def extract_main_warnings(run_id, sample_id_list, s3_bucket, prefix='.'):
    """
    Reads main_log files of run run_id and extracts warnings
    :param: run_id (str): ID of the run
    :param" sample_id_list (list(str)): sample ID list
    :param: s3_bucket (str): s3 bucket where to fetch the results
    :param: prefix (str): prefix of the output directory
    """
    def get_warning_out_path(run_id, prefix, warning_suffix):
        return os.path.join(out_dir(run_id, prefix),
                            f"{run_id}_warnings{warning_suffix}")

    for warning_suffix in WARNINGS_OUTPUT_SUFFIX.values():
        warning_out_path = get_warning_out_path(run_id, prefix, warning_suffix)
        warning_out = open(warning_out_path, 'w')
        warning_out.close()
    warning_out_file = {}
    for sample_id in sample_id_list:
        for warning_key, warning_suffix in WARNINGS_OUTPUT_SUFFIX.items():
            warning_out_path = get_warning_out_path(run_id, prefix,
                                                    warning_suffix)
            warning_out_file[warning_key] = open(warning_out_path, 'a')
        main_log_name = f"{run_id}_{sample_id}{MAIN_LOG_FILE_SUFFIX}"
        main_log_path = os.path.join('s3://', s3_bucket, run_id, main_log_name)
        main_log_file = open(main_log_path, 'r').readlines()
        for line in main_log_file:
            line_split = line.rstrip().split('\t')
            if line_split[1] == '[WARNING]':
                step, sample_amplicon = line_split[2].split()
                msg = line_split[3]
                if sample_amplicon[0:5].lower() != 'blank':
                    warning_out_file[step].write(f"{sample_amplicon}\t{msg}\n")
        for warning_key, warning_out in warning_out_file.items():
            warning_out.close()


if __name__ == "__main__":
    """
    Reads the input log from a set of runs and checks for each run that was
    processed if
    - the output directory in the S3 bucket (default cchauve-orchestration-ch)
      does exist
    - all the expected files (results and logs) are in the directory
    - all the log files are complete
    If any of these conditions is not met, the run ID and run name are added to
    the list of unprocessed runs that is written in a CSV file to be ran later.
    Otherwise, the warnings of the main log files are extrcated and the indels
    VCF are dumped into a single file.
    The extracted warnings and dumped VCF files are in the directory
    output_dir/run_id
    The log is in input_log_file.replace(_input.log, _output.log)
    The CSV file of runs to re-launch is in data and has the same name than the
    log file where _output.log is replaced by _failed.csv.
    For each successful run, the script stores in output_dir/run_id six TSV
    files:
    - <run_id>_indels_dump.tsv: indels calls in short format
    - <run_id>_warnings_reads.tsv: warnings raised while processing reads
    - <run_id>_warnings_clusters.tsv: warnings raised while creating read
      clusters
    - <run_id>_warnings_alignments.tsv: warnings raised while aligning reads
    - <run_id>_warnings_variants.tsv: warnings raised while detecting variants
      from alignments
    - <run_id>_warnings_variants_graph.tsv: warnings raised while creating
      variants graphs.

    Arguments:
    - input_log_file: input log file from a set of runs
    - output_dir: directory where the results are written
    - s3_bucket (optional, default cchauve-orchestration-ch): bucket where to
      fetch indels pipeline output files.
    """
    # Input file
    ARGS_RUNS_FILE = ['input_log_file', None, 'Input log file']
    # Results directory
    ARGS_OUTPUT_DIR = ['output_dir', None, 'Output directory']
    # S3 bucket containing the reuslts
    ARGS_S3_BUCKET = ['-s3', '--s3_bucket', 'S3 bucket containing the results']
    parser = argparse.ArgumentParser(
        description='Indels pipeline: analysis of results on AWS')
    parser.add_argument(ARGS_RUNS_FILE[0], type=str, help=ARGS_RUNS_FILE[2])
    parser.add_argument(ARGS_OUTPUT_DIR[0], type=str, help=ARGS_OUTPUT_DIR[2])
    parser.add_argument(ARGS_S3_BUCKET[0],
                        ARGS_S3_BUCKET[1],
                        default=CCHAUVE_S3_OUTPUT,
                        type=str,
                        help=ARGS_S3_BUCKET[2])
    args = parser.parse_args()

    log_file_path = args.input_log_file.replace('_input.log', '_output.log')
    log_file = open(log_file_path, 'w')

    (sample_id_lists,
     unprocessed_runs) = read_input_log_file(args.input_log_file)

    for (run_id, run_name), sample_id_list in sample_id_lists.items():
        os.makedirs(out_dir(run_id, args.output_dir), exist_ok=True)
        s3_files = get_files_in_s3(run_id, args.s3_bucket)
        if s3_files is None:
            log_file.write(f"{WARNING}:{run_id}\tno output\n")
            unprocessed_runs.append((run_id, run_name))
        elif not check_output_files(run_id, sample_id_list, s3_files):
            log_file.write(f"{WARNING}:{run_id}\tmissing output files\n")
            unprocessed_runs.append((run_id, run_name))
        elif not check_log_files(run_id, sample_id_list, args.s3_bucket):
            log_file.write(f"{WARNING}:{run_id}\tincomplete log file\n")
            unprocessed_runs.append((run_id, run_name))
        else:
            log_file.write(f"{INFO}:{run_id}\t{ERROR_NONE}\n")
            # Extracting warnings
            extract_main_warnings(run_id,
                                  sample_id_list,
                                  args.s3_bucket,
                                  prefix=args.output_dir)
            # Extracting indels calls
            extract_vcf_files(run_id,
                              sample_id_list,
                              args.s3_bucket,
                              log_file,
                              v_type=INDELS,
                              prefix=args.output_dir,
                              to_dump=True,
                              to_keep=False)
    # Exporting the list of runs to reprocess
    _, log_file_name = os.path.split(log_file_path)
    unprocessed_file_path = os.path.join(
        'data', log_file_name.replace('_output.log', '_failed.csv'))
    unprocessed_file = open(unprocessed_file_path, 'w')
    unprocessed_run_first = True
    for (run_id, run_name) in unprocessed_runs:
        if unprocessed_run_first:
            unprocessed_file.write(f"{run_name},{run_id}")
            unprocessed_run_first = False
        else:
            unprocessed_file.write(f"\n{run_name},{run_id}")
    unprocessed_file.close()
    log_file.close()
