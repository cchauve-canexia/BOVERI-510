# BOVERI-510
## Process testing runs with indel NextFlow pipeline

This repo contains scripts to launch AWs jobs for the indels pipeline and
retrieve the results.

The repo contains two main scripts:
- bin/run_utils.py to launch AWS jobs
- bin/analysus_utils.py to retrieve results

To start jobs:
bin/run_utils runs_csv_file branch
where run_csv_file is a csv file with two fields: Run, ID
(e.g. CG001Qv40Run10,180808_M03829_0152_000000000-BWL8C)
The script parses the beginning of Run to identify the amplicon manifest to use.

To run it on the runs for testing MSI amplicons:
bin/run_utils data/Runs_to_run_indel_caller_with_MSI_amplicons.csv BOVERI-515

The retrieve results
bin/analysis_utils runs_csv_file output_dir -s3 s3_bucket[OPTIONAL]
s3_bucket is an optional parameter (default 'cchauve-orchestration-ch')
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
