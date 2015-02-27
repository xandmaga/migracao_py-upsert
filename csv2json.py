import csv
import sys
import json
import codecs

def convert(filename):
  csv_filename = filename[0]
  print "Opening CSV file: ",csv_filename
  f = open(csv_filename, 'r') #codecs.open(csv_filename, 'r', 'utf-8')
  csv_reader = csv.DictReader(f)  
  json_filename = csv_filename.rpartition('.')[0]+".json"
  print "Saving JSON to file: ",json_filename
  jsonf = open(json_filename,'w')
  data = json.dumps([r for r in csv_reader])
  jsonf.write(data)
  f.close()
  jsonf.close()

def carrega_arquivo(arquivo):
  with open(arquivo) as json_file:
      return json.load(json_file)  

if __name__=="__main__":
  convert(sys.argv[1:])
