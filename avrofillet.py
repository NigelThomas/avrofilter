import avro.schema
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("avro.schema", "rb").read())

reader = DataFileReader(open("cdn.avro","rb"), DatumReader(schema))

writer = DataFileWriter(open("cdn-new.avro","wb"), DatumWriter(),schema)

line = 0
linescopied = 0
for row in reader:
    if (line > 0):
        writer.append(row)
        linescopied += 1 
    else:
        print (row)
    line += 1

print (str(linescopied)+" lines copied")

writer.close()
reader.close()
      
