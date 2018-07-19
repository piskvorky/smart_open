import os
import os.path as P

import avro.io
import avro.datafile
import pandas as pn
import smart_open

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'

if not P.isfile('index_2018.csv'):
    os.system('aws s3 cp s3://irs-form-990/index_2018.csv .')

with open('index_2018.csv') as fin:
    data = pn.read_csv(fin, header=1, error_bad_lines=False).fillna('NA')

schema = gen_schema(xa.columns)

output_url = _S3_URL + '/issue_209/out.avro'

with smart_open.smart_open(output_url, 'wb') as foutd:
    dictRes = data.to_dict(orient='records')
    writer = avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema)
    for ll, row in enumerate(dictRes):
        writer.append(row)
