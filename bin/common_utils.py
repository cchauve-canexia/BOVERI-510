"""
Constants common to both modules
"""

import boto3

ERROR_RUN_NO_DATA = 'no data'
ERROR_RUN_NO_SAMPLE = 'no sample'
ERROR_RUN_NO_CORRECT_SAMPLE = 'no correct sample'
ERROR_RUN_UNPROCESSED = 'unprocessed'
ERROR_SAMPLE_FASTQ = 'error FASTQ files'
ERROR_SAMPLE_NONE = 'OK'
WARNING = 'WARNING'
RUN_ID = 'RUN.ID'
AWS_CMD = 'AWS'
RUN_SAMPLES = 'RUN.SAMPLES'


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
