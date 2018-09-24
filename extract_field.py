# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import argparse
import csv
import json
import pyarrow.parquet as pq

from ethereumetl.csv_utils import set_max_field_size_limit
from ethereumetl.file_utils import smart_open

parser = argparse.ArgumentParser(description='Extracts a single field from a given file.')
parser.add_argument('-i', '--input', default='-', type=str, help='The input file. If not specified stdin is used.')
parser.add_argument('-o', '--output', default='-', type=str, help='The output file. If not specified stdout is used.')
parser.add_argument('-f', '--field', required=True, type=str, help='The field name to extract.')

args = parser.parse_args()

if args.input.endswith('.csv'):
    set_max_field_size_limit()
    with smart_open(args.input, 'r') as input_file, smart_open(args.output, 'w') as output_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            output_file.write(row[args.field] + '\n')

elif args.input.endswith('.json'):
    with smart_open(args.input, 'r') as input_file, smart_open(args.output, 'w') as output_file:
        for line in input_file:
            item = json.loads(line)
            output_file.write(item[args.field] + '\n')

elif args.input.endswith('.parquet'):
    with smart_open(args.output, 'w') as output_file:
        lst = pq.read_table(args.input).column(args.field).to_pandas().tolist()
        for each in lst:
            output_file.write(each + '\n')
