#!/usr/bin/env python3
"""
Add aliquots informations to a dump TSV file
"""

# Standard imports
import argparse
import csv
from collections import defaultdict

# Local imports
from common_utils import VCF_DUMP_FIELDS_SEP, VCF_DUMP_VALUES_SEP

if __name__ == "__main__":
    """
    Arguments:
    - input_tsv_file: input dump TSV file with samples list
    - output_tsv_file: output dump TSV file with added aliquot information
    """
    # Input file
    ARGS_IN_FILE = ['input_tsv_file', None, 'Input tsv file']
    ARGS_OUT_FILE = ['output_tsv_file', None, 'Output tsv file']
    description = 'Indels pipeline: '
    description += 'add aliquots information to an aggregated dump TSV file'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(ARGS_IN_FILE[0], type=str, help=ARGS_IN_FILE[2])
    parser.add_argument(ARGS_OUT_FILE[0], type=str, help=ARGS_OUT_FILE[2])
    args = parser.parse_args()

    out_file = open(args.output_tsv_file, 'w')
    header = 'nb_samples\tnb_aliquots\tchr\tpos\tref\talt\tannotation\t'
    header += 'avg_vaf\tstd_vaf\taliquots:count\tsamples:vaf'
    out_file.write(f"{header}\n")
    out_file.close()
    indels_ext = []
    with open(args.input_tsv_file) as in_tsv:
        indels = csv.reader(in_tsv, delimiter='\t')
        for indel in indels:
            if indel[0] != 'nb':
                samples = indel[8].split(',')
                aliquots_count = defaultdict(int)
                for sample in samples:
                    aliquot = sample.split('-')[1]
                    aliquots_count[aliquot] += 1
                aliquots_list_aux = [
                    (count, f"{aliquot}:{count}")
                    for aliquot, count in aliquots_count.items()
                ]
                aliquots_list_aux.sort(key=lambda x: x[0], reverse=True)
                aliquots_list = [x[1] for x in aliquots_list_aux]
                aliquots_nb = len(aliquots_list)
                indel_ext = [indel[0], aliquots_nb]
                indel_ext += indel[1:8]
                indel_ext += [VCF_DUMP_VALUES_SEP.join(aliquots_list)]
                indel_ext += [indel[8]]
                indels_ext.append(indel_ext)
    with open(args.output_tsv_file, 'a') as out_file:
        writer = csv.writer(out_file, delimiter=VCF_DUMP_FIELDS_SEP)
        writer.writerows(indels_ext)
