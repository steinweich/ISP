#!/usr/bin/env python3

# Library for reading csv files
import csv

# Library for reading command line parameters
import sys

#Library for (file-)system operations
import os


# ====================================================================== Control flow begin
# Declare all global variables (this is not neccessary - just for overview purposes)
input_filename = None
input_filename_has_header = False

output_filename = None

original_data = []
original_columns = []

# ---------------------------------------------------------------------- Checking command line arguments

if len(sys.argv) >= 3:
  input_filename = sys.argv[1]
  output_filename = sys.argv[2]
else:
  print("Please specify at least two arguments: input file and output file")
  exit(1)

for i in range(3,len(sys.argv)):
  if sys.argv[i] == "-h":
    input_filename_has_header = True

if not os.path.isfile(input_filename):
  print("Specified input file does not exist")
  exit(1)

if os.path.isfile(output_filename):
  print("Specified output file does exist: Overwrite? (Y): ")
  answer = raw_input()
  if answer == "" or answer.upper() == "Y":
    pass
  else:
    print("Abort by user")
    exit(0)

# ---------------------------------------------------------------------- Read data from csv file

input_file = open(input_filename, 'r')
csvreader = csv.reader(input_file)

first = True
j=0
for row in csvreader:
  if first:
    first = False
    # Initialise the column information
    for i in range(len(row)):
      field = row[i]
      header_data = {'float':True, 'name':str(i), 'max':None, 'min':None, 'missing_values':[], 'mean':0.0}
      if input_filename_has_header:
        header_data['name'] = field
      original_columns.append(header_data)
    
    if input_filename_has_header:
      continue
  
  # Read the data and transform the string to float where possible
  newrow = []
  for i in range(len(row)):
    field = row[i]
    column = original_columns[i]
    
    if field == None or field == '':
      column['missing_values'].append(j)
    else:
      if column['float']:
        try:
          field = float(field)
          if column['min'] == None or field < column['min']:
            column['min'] = field
          if column['max'] == None or field > column['max']:
            column['max'] = field
          column['mean'] += field
        except ValueError:
          column['float'] = False
          column['mean'] = 0
          pass
    newrow.append(field)
  
  original_data.append(newrow)
  j+=1

for i in range(len(original_columns)):
  column = original_columns[i]
  if column['float']:
    column['mean'] = column['mean'] / (len(original_data) - len(column['missing_values']))  
  
  print(column)
