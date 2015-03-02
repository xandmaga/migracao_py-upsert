#!/usr/local/bin/python

import os
import sys
import argparse
import re
import types
import csv
import json
import logging

logging.basicConfig(format='\033[1;36m%(levelname)s:\033[0;37m %(message)s', level=logging.DEBUG)

parser = argparse.ArgumentParser()
parser.add_argument('csv_filename', help='Name of the csv file you want to convert to json')
args = parser.parse_args()

def processCsvForConversion(filename):
    print "parsing %s \n" % (filename)
    file = open(filename, 'rU')
    fieldnames = []
    dict_keys = file.readline()
    split_keys = dict_keys.rstrip('\r\n').split(',')
    for key in split_keys:
        if ' ' in key:
            edited_key = key.replace(" ", "").lower()
            fieldnames.append(edited_key)
        else:
            edited_key = key.lower()
            fieldnames.append(edited_key)
    fieldnames = tuple(fieldnames)
    csvInstance = csv.DictReader(file, fieldnames = fieldnames, quoting=csv.QUOTE_NONE)
    process(csvInstance, filename)
    file.close()

'''
def process(csvInstance, filename):
    jsonOutput = json.dumps([row for row in csvInstance])

    if args.usage == 'timeline':
        print "creating json for timeline"
        data_to_write = '%s' % (jsonOutput)
        json_flat_file = os.path.splitext(filename)[0] + '_timeline.json'

    elif args.usage == 'array':
        print "creating array of objects"
        data_to_write = '%s' % (jsonOutput)
        #json_filename = csv_filename.rpartition('.')[0]+".json"
        #json_flat_file = os.path.splitext(filename)[0] + '_array.json'
        json_flat_file = filename.rpartition('.')[0]+".json"
        

    else:
        print "creating json for handlebars template"
        data_to_write = '{"objects": %s}' % jsonOutput
        json_flat_file = os.path.splitext(filename)[0] + '_handlebars.json'
'''

def process(csvInstance, filename):
    jsonOutput = json.dumps([row for row in csvInstance])

    print "creating array of objects"
    data_to_write = '%s' % (jsonOutput)
    #json_filename = csv_filename.rpartition('.')[0]+".json"
    #json_flat_file = os.path.splitext(filename)[0] + '_array.json'
    json_flat_file = filename.rpartition('.')[0]+".json"
    
    new_file = open(json_flat_file, 'w')
    new_file.write(data_to_write)
    new_file.close()
    print '%s converted to %s \n' % (filename, json_flat_file)
    return

if __name__ == '__main__':
    processCsvForConversion(args.csv_filename)