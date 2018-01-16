#!/usr/bin/env python3

# Library for reading csv files
import csv

# Library for reading command line parameters
import sys

#Library for (file-)system operations
import os
import os.path

def print_help():
  print('Arguments:')
  print('Inputfile')
  print('Key Column Name')
  print('Class Column Name')
  print('Data Column Indexes (fromindex-toindex)')
  exit(1)

data = []

if len(sys.argv) > 1:
  input_filename = sys.argv[1]
else:
  print('Need input file to work on')
  print_help()

if len(sys.argv) > 2:
  key_column_name = sys.argv[2]
else:
  print('Need key column name')
  print_help()

if len(sys.argv) > 3:
  class_column_name = sys.argv[3]
else:
  print('Need Class column name')
  print_help()

if len(sys.argv) > 4:
  to_index = sys.argv[4]
  from_index = to_index
  if to_index.find('-') > -1:
    cols = to_index.split("-")
    from_index = cols[0]
    to_index = cols[1]
  try:
    to_index = int(to_index)
    from_index = int(from_index)
  except ValueError:
    print('Could not read data column indexes')
    exit(1)
else:
  print('Need data Column index to extract data')
  print_help()

if not os.path.isfile(input_filename):
  print("Inputfile does not exists: " + input_filename)
  exit(1)

input_file = open(input_filename, 'r')
csvreader = csv.reader(input_file)
filesize = os.path.getsize(input_filename) / (1024 ** 2)

print('Reading data from file ' + input_filename + ' [' + str(filesize) + ']')

first = True  # To know wheter we're processing the first row

key_column_index = None
class_column_index = None
data_column_index = None
classes = []
out_data = {}

header = None

for row in csvreader:
  if first:
    first = False
    # Initialise the column information
    header = row
    for i in range(len(row)):
      field = row[i]
      if row[i] == class_column_name:
        class_column_index = i
      elif row[i] == key_column_name:
        key_column_index = i
      
    if class_column_index == None or key_column_index == None:
      print('Could not find one of the specified indexes!')
      print("Available columns: ")
      print(row)
      exit(1)
    
    print('Will extract data from columns: ')
    for i in range(from_index, to_index+1):
      print(row[i])
      out_data[row[i]] = {}
    input('Enter to continue')
    
  else:
    key = row[key_column_index]
    cl = row[class_column_index]
    
    for data_column_index in range(from_index, to_index+1):
    
      val = row[data_column_index]
      
      if cl not in classes:
        classes.append(cl)
      
      if key not in out_data[header[data_column_index]]:
        out_data[header[data_column_index]][key] = {}
      
      out_data[header[data_column_index]][key][cl] = val

print("... Done")


for col in out_data:
  od = out_data[col]
  output_filename = input_filename[:-4] + "_" + col + ".csv"
  if os.path.isfile(output_filename):
    print('Skipping ' + col + ' : file already exists: ' + output_filename)
    continue

  print("Writing data to file " + output_filename + " ...")
  output_file = open(output_filename, 'w')
  csvwriter = csv.writer(output_file)
  
  header = [key_column_name]
  for cl in classes:
    header.append(cl)
  csvwriter.writerow(header)
  
  for key in od:
    data = od[key]
    row = [key]
    for cl in classes:
      if cl in data:
        row.append(data[cl])
      else:
        print('Could not transform: ' + key)
        print(data)
        exit(1)
    csvwriter.writerow(row)
  
  output_file.close()
  print("... Done")
  
