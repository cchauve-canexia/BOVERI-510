# BOVERI-510
## Process testing runs with indel NextFlow pipeline

This repo contains scripts to launch AWS jobs for the indels pipeline and
retrieve the results.

The repo contains three main scripts:  
- bin/run_utils.py to launch AWS jobs  
- bin/analysis_utils.py to retrieve results  
- bin/extract_colocated_indels.py
- bin/aggregate_dump_files.py
- bin/retrieve_run.py
- bin/count_samples.py

### run_utils
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

It checks the directory <s3_input>/input/<run_id> for each run and looks into
every subdirectory ending by -XX_SYY where XX and YY are integers (not assumed to
be equal) that there are only two files, that are two gzipped raw FASTQ files
Any run
- with a directory whose name does not match the definition of a sample ID, or  
- with a sample directory without only the two FASTQ files,  
is not processed.  

It generates a log file log/run_csv_file ".csv" replaced by "_input.log" indicating
processed runs and unprocessed runs. Errors in the log file are prefixed by
WARNING.

### analysis_utils
The script bin/analysis_utils.py reads the input log from a set of runs
(generated by bin/run_utils.py)and checks for each run that was
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

Arguments:
- input_log_file: input log file from a set of runs
- output_dir: directory where the results are written
- s3_bucket (optional, default cchauve-orchestration-ch): bucket where to
  fetch indels pipeline output files.

For each successful run, the script stores in output_dir/run_id six TSV files:  
- <run_id>_indels_dump.tsv: indels calls in short format
- <run_id>_warnings_reads.tsv: warnings raised while processing reads
- <run_id>_warnings_clusters.tsv: warnings raised while creating read clusters
- <run_id>_warnings_alignments.tsv: warnings raised while aligning reads
- <run_id>_warnings_variants.tsv: warnings raised while detecting variants from
  alignments
- <run_id>_warnings_variants_graph.tsv: warnings raised while creating variants
  graphs.

### extract_colocated_indels
The script bin/extract_colocated_indels.py reads the output log file for a set
of runs, reads the dump file for each successful run and detects groups of
co-located indels. It generates a text file containing each such group and the
samples where it was detected.

Arguments:
- output_log_file: path to the output log file for a set of runs
- output_dir: path to the directory containing the output of the runs
- output_file: path to the file that contains the groups of co-located
  indels
- gap_len (optional): integer defining ghe maximum gap between consecutive
  indels to put them into the same group; default = 5

### aggregate_dump_files
The script aggregates all TSV dump files for indels into a aggregated TSV dump files
for a set of runs. For a set of runs, it splus the samples in three groups:
- patient samples (name starts by DNA)
- control samples (name starts by NF, QMRS or BLANK)
- misc samples (all other cases)
For each group it generates 2 TSV files:
- <group>_samples_indels_dump.tsv
  one line per individual call, in the same format than the run-specific dump files
- <group>_grouped_samples_indels_dump.tsv
  calls are grouped by the key (chr, position, reference, alternate)
  and for each group it shows in how many samples it occurs, the mean and standard
  deviation of the VAF, and the samples it occurs in (with the corresponding VAF).

Arguments:
 - output_dir: directory where to fetch the run-specific dump files and write the
   aggregated dump files.

### retrieve_run
The script downloads from an S3 bucket all main.tar.gz and vcf.tar.gz files for a
given run.

Arguments:
- run_id: ID of the run
- output_dir: files are downloaded and unarchived in this directory, in a
  subdirectory run_id
- s3: bucket where to fetch the files (in directory run_id)

### count_samples
Counts the number of samples of each group in a set of runs.

Arguments:
-  input_log_file: input log file from a set of runs
