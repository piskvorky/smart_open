import os
import os.path as P

import avro.io
import avro.datafile
import pandas as pn
import smart_open

_S3_URL = os.environ.get('SO_S3_URL')
assert _S3_URL is not None, 'please set the SO_S3_URL environment variable'

def gen_schema(paramNames):
	paramNamesLen = len(paramNames)
	

	dataName = 'schema'
	avroSchemaOut = "{\n\t\"type\": 	\"record\", \"name\": \"%s\", \"namespace\": \"com.sandisk.bigdata\", \n \t\"fields\": [" %(dataName)  
	

	if paramNamesLen==0:
		#no parameters, no schema file generation
		avroSchemaOut = ''
	   
	else:
		#generate file
		for ii in range(paramNamesLen):
			typeString = "[\"%s\", \"null\"]" %('String')
			schemaString = "{ \"name\":\"%s\", \"type\":%s, \"default\":null}" % (paramNames[ii], typeString)
			if ii == 0:
				avroSchemaOut += schemaString + ',\n'
			elif ii <len(paramNames)-1:
				avroSchemaOut += "\t\t\t" + schemaString + ',\n'
			else:
				avroSchemaOut += "\t\t\t" + schemaString + '\n'
		avroSchemaOut += "\n \t\t\t]\n}"
		
	return avroSchemaOut

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
