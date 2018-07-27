import os
import os.path as P
import warnings

import avro.io
import avro.datafile
import smart_open
import six

with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    import pandas as pn

if six.PY3:
    assert False, 'this code only runs on Py2.7'

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'

_NUMROWS = os.environ.get('SO_NUMROWS')
if _NUMROWS is not None:
    _NUMROWS = int(_NUMROWS)


def gen_schema(paramNames, dataName, paramTypes=''):
    paramNamesLen = len(paramNames)
    paramTypeLen = len(paramTypes)

    if paramTypeLen > 0 and paramNamesLen <> paramTypeLen:
        raise('There is an issue with parameter type length! fix it!')

    avroSchemaOut = "{\n\t\"type\":     \"record\", \"name\": \"%s\", \"namespace\": \"namespace\", \n \t\"fields\": [" % (
        dataName)

    if paramNamesLen == 0:
        # no parameters, no schema file generation
        avroSchemaOut = ''

    else:

        for ii in range(paramNamesLen):
            if paramTypeLen < 1:
                typeString = "[\"null\",\"string\"]"
            else:
                typeString = "[\"%s\", \"null\"]" % paramTypes[ii]
            schemaString = "{ \"name\":\"%s\", \"type\":%s, \"default\":null}" % (
                paramNames[ii], typeString)
            if ii == 0:
                avroSchemaOut += schemaString + ',\n'
            elif ii < len(paramNames)-1:
                avroSchemaOut += "\t\t\t" + schemaString + ',\n'
            else:
                avroSchemaOut += "\t\t\t" + schemaString + '\n'
        avroSchemaOut += "\n \t\t\t]\n}"

    return avroSchemaOut


if not P.isfile('index_2018.csv'):
    os.system('aws s3 cp s3://irs-form-990/index_2018.csv .')

with open('index_2018.csv') as fin:
    data = pn.read_csv(fin, header=0, error_bad_lines=False,
                       nrows=_NUMROWS, dtype='str').fillna('NA')

avroSchemaOut = gen_schema(data.columns, dataName='data', paramTypes='')

with open('schema.out', 'wb') as fout:
    fout.write(avroSchemaOut)

output_url = _S3_URL + '/issue_209/out.avro'


def write_avro(foutd):
    schema = avro.schema.parse(avroSchemaOut)
    dictRes = data.to_dict(orient='records')
    writer = avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema)
    for ll, row in enumerate(dictRes):
        writer.append(row)


with smart_open.smart_open(output_url, 'wb') as foutd:
    write_avro(foutd)

with open('local.avro', 'wb') as foutd:
    write_avro(foutd)

os.system('aws s3 cp %s remote.avro' % output_url)

os.system('diff local.avro remote.avro')
