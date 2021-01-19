# BOVERI-510
## Process testing runs with indel NextFlow pipeline

This repo contains scripts to launch AWS jobs for the indels pipeline and
retrieve the results.

The repo contains two main scripts:
- bin/run_utils.py to launch AWS jobs
- bin/analysus_utils.py to retrieve results

To start jobs:  
bin/run_utils runs_csv_file s3_bucket_input branch -o s3_bucket_output[OPTIONAL] -d aws_def[OPTIONAL] -q aws_queue[OPTIONAL]  
where  
- run_csv_file is a csv file with two fields: Run, ID
  (e.g. CG001Qv40Run10,180808_M03829_0152_000000000-BWL8C)
  for MSI: data/Runs_to_run_indel_caller_with_MSI_amplicons.csv
- branch is a branch of indels-pipeline (for MSI: BOVERI-515)
- s3_bucket_output is the s3 directory that will contain one directory per run with the output
  (default value in nextflow.config in repo indels-pipeline)
- aws_def is the value for the option --job-definition of aws
  (default: cchauve)
- aws_queue is the value for the option --job-queue of aws
  (default: cchauve-orchestration-default)  
The script parses the beginning of Run to identify the amplicon manifest to use.

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
