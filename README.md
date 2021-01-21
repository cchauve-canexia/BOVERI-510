# BOVERI-510
## Process testing runs with indel NextFlow pipeline

This repo contains scripts to launch AWS jobs for the indels pipeline and
retrieve the results.

The repo contains two main scripts:
- bin/run_utils.py to launch AWS jobs
- bin/analysis_utils.py to retrieve results


The script bin/run_utils.py checks the input data for a list of runs and submits
AWS jobs for each valid run.

Arguments:
- runs_csv_file: CSV file with 2 fields <run_name>,<run_id>
  run_name is used to define the amplicon manifest
- s3_input: S3 bucket containing the runs input data
  assumed structure of a run input directory:
  configuration files and set of subdirectories
  <s3_input>/input/<run_id>/<sample_id>/<sample_id>_L001_[R1,R2]_001.fastq.gz
- branch: branch of the indels-pipeline repo to use (currently BOVERI-448-nf or
  BOVERI-515 for MSI amplicons)
- s3_output: S3 bucket where to store the results
  results for run_id go into <s3_output>/<run_id>
  default value in nextflow.config
- aws_def: --job-definition value for aws
  default value: cchauve (AWS_DEF)
- aws_queue: queue to send the jobs to, --job-queue value for aws
  default value: cchauve-orchestration-default (AWS_QUEUE)

Checks the directory <s3_input>/input/<run_id> for each run and looks into
every subdirectory ending by -XX_SYY where XX and YY are integers (not assumed to
be equal) that there are only two files, that are two gzipped raw FASTQ files
Any run
- with a directory whose name does not match the definition of a sample ID,  or
- with a sample directory without only the two FASTQ files,
is not processed.

Generates a log file indicating processed runs and unprocessed runs.
Errors are prefixed by WARNING.


To retrieve results  
bin/analysis_utils runs_csv_file output_dir -s3 s3_bucket[OPTIONAL]  
where s3_bucket is an optional parameter (default 'cchauve-orchestration-ch')
that specifies in which directory of s3:// to fetch the results of the indels
pipeline.


For each run in runs_csv_file, the script stores in output_dir/run_id six TSV
files:  
- <run_id>_indels_dump.tsv: indels calls in short format
- <run_id>_warnings_reads.tsv: warnings raised while processing reads
- <run_id>_warnings_clusters.tsv: warnings raised while creating read clusters
- <run_id>_warnings_alignments.tsv: warnings raised while aligning reads
- <run_id>_warnings_variants.tsv: warnings raised while detecting variants from
  alignments
- <run_id>_warnings_variants_graph.tsv: warnings raised while creating variants
  graphs.
