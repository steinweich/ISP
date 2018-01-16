#!/usr/bin/env python3

# Library for reading csv files
import csv

# Library for reading command line parameters
import sys

#Library for (file-)system operations
import os
import os.path

#Library to use sqlite3
import sqlite3

# ====================================================================== Control flow begin
# Declare all global variables (this is not neccessary - just for overview purposes)
input_filename = None
input_filename_has_header = False

output_filename = None

#original_data = []
original_columns = []
original_data_count = 0
id_column_index = None
marked = []
unmarked = []

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
  answer = input()
  if answer == "" or answer.upper() == "Y":
    pass
  else:
    print("Abort by user")
    exit(0)

# ---------------------------------------------------------------------- Prepare Database

database = sqlite3.connect(":memory:")
cursor = database.cursor()

# ---------------------------------------------------------------------- Read data from csv file

input_file = open(input_filename, 'r')
csvreader = csv.reader(input_file)
filesize = os.path.getsize(input_filename) / (1024 ** 2)

print('Reading data from file ' + input_filename + ' [' + str(filesize) + ']')

first = True  # To know wheter we're processing the first row
j=0 # Counter for the datasets
for row in csvreader:
  if first:
    first = False
    sql = []
    # Initialise the column information
    for i in range(len(row)):
      field = row[i]
      header_data = {'float':True, 'name':None, 'max':None, 'min':None, 'missing_values':[], 'mean':0.0, 'values':{}}
      if input_filename_has_header:
        header_data['name'] = field.replace(' ','').strip()
        sql.append(header_data['name'])
      else :
        sql.append("COL" + str(i))
      if header_data['name'] == 'id':
        id_column_index = i
      original_columns.append(header_data)
    
    if id_column_index == None:
      header_data = {'float':False, 'name':'id', 'max':None, 'min':None, 'missing_values':[], 'mean':0.0, 'values':{}}
      original_columns.insert(0, header_data)
      sql.insert(0, 'id')
      id_column_index = 0
    
    sql = "CREATE TABLE data ([" + "],[".join(sql) + "])"
    cursor.execute(sql)
    
    if input_filename_has_header:
      continue
  # Read the data and transform the string to float where possible
  if len(row) < len(original_columns):
    row.insert(0, str(j))
  else:
    j = row[id_column_index]
  
  unmarked.append(str(j))
  
  newrow = []
  for i in range(len(row)):
    field = row[i].strip()
    column = original_columns[i]
    
    if field == None or field == '':
      column['missing_values'].append(j)
    else:
      if column['float']:
        try:
          field = float(field)
          # Collect statistics about the data
          if column['min'] == None or field < column['min']:
            column['min'] = field
          if column['max'] == None or field > column['max']:
            column['max'] = field
          column['mean'] += field
        
        except ValueError:
          column['float'] = False
          column['mean'] = 0
          pass
    
      if str(field) not in column['values']:
        column['values'][str(field)] = []
      column['values'][str(field)].append(j)
    
    newrow.append(field)
  
  #original_data.append(newrow)
  cursor.execute("INSERT INTO data VALUES (" + ','.join(['?'] * len(original_columns)) + ")", newrow)
  original_data_count += 1
  j+=1
database.commit()

# Compute the mean and output the statistics
for i in range(len(original_columns)):
  column = original_columns[i]
  if column['float']:
    if original_data_count - len(column['missing_values']) > 0:
      column['mean'] = column['mean'] / (original_data_count - len(column['missing_values']))
    else:
      column['mean'] = 0

# Command Loop
command = None

while command != 'EXIT' and command != 'QUIT':
  print('========== ========== ========== ========== ==========')
  for i in range(len(original_columns)):
    column = original_columns[i]
    print(str(i), end=' '),      
    if column['name']:
      print(column['name'], end=''),
    if column['float']:
      print("\tMIN: " + str(column['min']) + " MAX: " + str(column['max']) + "\tMEAN: " + str(column['mean']), end='')
    if original_data_count > 0:
      print("\tMISSING: " + str(int(float(len(column['missing_values'])) / original_data_count * 100)) + '% [' + str(len(column['missing_values'])) + ']', end='')
    else:
      print("\tMISSING: 0% [" + str(len(column['missing_values'])) + ']', end='')
    print("\tVALUES: " + str(len(column['values'])))
    
  original_command = input(str(original_data_count) + '/' + str(len(marked)) + "> ")
  command = original_command.upper()
  work_done = False
  argv = command.split(' ')
  oargv = original_command.split(' ')
  recalculate = False
  
  print('---------- ---------- ---------- ---------- ----------')
  
  if len(argv) > 1 and argv[0] == 'SQL': # ----------------------------- SQL
    work_done = True
    sql = original_command[4:]
    try:
      print('Executing >' + sql + '<')
      cursor.execute(sql)
      
      if argv[1] == 'SELECT':
        for row in cursor.fetchall():
          print(row)
      if command.find('UPDATE') > -1 or command.find('DELETE') > -1:
        recalculate = True
        database.commit()
      print('Done')
    except sqlite3.Error as e:
      print('An error occurred: ' + str(e.args[0]))
  elif len(argv) == 1:  #11111111111111111111111111111111111111111111111
    if argv[0] == 'REC':# ---------------------------------------------- RECALCULATE
      recalculate = True
      work_done = True
    elif argv[0] == 'SAVE': # ------------------------------------------ SAVE
      print('SAVING TO ' + output_filename)
      output_file = open(output_filename, 'w')
      csvwriter = csv.writer(output_file)
      header = []
      for i in range(len(original_columns)):
        if i != id_column_index:
          header.append(original_columns[i]['name'])
      csvwriter.writerow(header)
      
      cursor.execute("SELECT [" + "],[".join(header) + "] FROM data")
      for row in cursor.fetchall():
        csvwriter.writerow(row)
      output_file.close()
      work_done = True
    elif argv[0] == 'MINV':
      print('INVERSE MARKED ENTRIES')
      tm = marked
      marked = unmarked
      unmarked = tm
      work_done = True
    elif argv[0] == 'MRES':
      print('RESET MARKS')
      marked = []
      unmarked = []
      cursor.execute("SELECT id FROM data")
      for row in cursor.fetchall():
        unmarked.append(str(row[0]))
      work_done = True
    elif argv[0] == 'MSHOW':
      for m in marked:
        sql = "SELECT * FROM data WHERE id='" + str(m) + "'"
        cursor.execute(sql)
        print(cursor.fetchone())
        
      #print(marked)
      work_done = True
    elif argv[0] == 'USHOW':
      for m in unmarked:
        sql = "SELECT * FROM data WHERE id='" + str(m) + "'"
        cursor.execute(sql)
        print(cursor.fetchone())
      work_done = True
  elif len(argv) == 2: # 22222222222222222222222222222222222222222222222
    if argv[0] == 'VALUES': # ------------------------------------------ VALUES
      work_done = True
      try:
        column_index = int(argv[1])
      except ValueError:
        print('Not a valid option')
        column_index = None
      if column_index != None and column_index > -1 and column_index < len(original_columns):
        column = original_columns[column_index]
        values = list(column['values'].keys())
        values.sort()
        for v in values:
          print(str(v) + "\t" + str(int(float(len(column['values'][v])) / (original_data_count - len(column['missing_values'])) * 100)) + '% [' + str(len(column['values'][v])) + ']')
    elif argv[0] == 'DROP': # ------------------------------------------ DROP MARK
      if argv[1] == 'MARK':
        cursor.execute("DELETE FROM data WHERE id IN ('" + "','".join(marked) + "')")
        database.commit()
        recalculate = True
        marked = []
        work_done = True
      elif argv[1] == 'UNMARK':
        print('DROP UNMARKED DATA (' + str(len(unmarked)) + ')')
        tmp = []
        j = 0
        for i in range(len(unmarked)):
          tmp.append(unmarked[i])
          j += 1
          if j >= 1000:
            cursor.execute("DELETE FROM data WHERE id IN ('" + "','".join(tmp) + "')")
            print(str( int(float(i * 100) / len(unmarked))) + "%")
            j = 0
            tmp = []

        if len(tmp) > 0:
          cursor.execute("DELETE FROM data WHERE id IN ('" + "','".join(tmp) + "')")
        print("100%")
        
        print('Commit')
        database.commit()
        print('Done')
        recalculate = True
        unmarked = []
        work_done = True
    elif argv[0] == 'SCALE':  #----------------------------------------- SCALE
      fromcol = None
      tocol = None
      if argv[1].find("-") > -1:
        cols = argv[1].split("-")
        try:
          fromcol = int(cols[0])
          tocol = int(cols[1])
        except ValueError:
          print('Not a valid option')
      else:
        try:
          fromcol = int(argv[1])
          tocol = fromcol
        except ValueError:
          print('Not a valid option')
      
      if fromcol != None and tocol != None and fromcol > -1 and tocol > -1 and fromcol < len(original_columns) and tocol < len(original_columns):
        print('Scaling columns ' + str(fromcol) + ' to ' + str(tocol) + ' ...')
        maxval = None
        minval = None
        for scalecol in range(fromcol, tocol + 1):
          if original_columns[scalecol]['float']:
            sql = "SELECT MAX([" + original_columns[scalecol]['name'] + "]),MIN([" + original_columns[scalecol]['name'] + "]) FROM data WHERE [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
            cursor.execute(sql)
            row = cursor.fetchone()
            if row[0] != None and (maxval == None or float(row[0]) > maxval):
              maxval = float(row[0])
          
            if row[1] != None and (minval == None or float(row[1]) < minval):
              minval = float(row[1])

        
        if maxval != None and minval != None:
          
          print("... Found " + str(minval) + " - " + str(maxval))
          scaled = 0
          for scalecol in range(fromcol, tocol + 1):
            if original_columns[scalecol]['float']:
              if maxval != minval:
                sql = "UPDATE data SET [" + original_columns[scalecol]['name'] + "]=([" + original_columns[scalecol]['name'] + "] - '" + str(minval) + "') / ('" + str(maxval) + "' - '" + str(minval) + "' ) WHERE [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
              else:
                sql = "UPDATE data SET [" + original_columns[scalecol]['name'] + "]='0.5' WHERE [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
            
              #print(sql)
              cursor.execute(sql)
              recalculate = True
              scaled += 1
          print("... scaled " + str(scaled) + " columns")
        else:
          print("... Nothing todo")
      if recalculate:
        database.commit()
      print('DONE')
        
  elif len(argv) == 3: #333333333333333333333333333333333333333333333333
    if argv[0] == 'DROP': # -------------------------------------------- DROP
      if argv[1] == 'COL':
        work_done = True
        try:
          column_index = int(argv[2])
        except ValueError:
          print('Not a valid option')
          column_index = None
        
        if column_index !=None and column_index > 0 and column_index < len(original_columns) and column_index != id_column_index:
          print('Dropping column ' + str(column_index))
          newcols = []
          for i in range(len(original_columns)):
            if i != column_index:
              newcols.append(original_columns[i]['name'])
          cursor.execute("CREATE TABLE tmp ([" + "],[".join(newcols) + "])")
          
          cursor.execute("INSERT INTO tmp SELECT [" + "],[".join(newcols) + "] FROM data")
          cursor.execute("DROP TABLE data")
          cursor.execute("ALTER TABLE tmp RENAME TO data")
          database.commit()
          del original_columns[column_index]
      elif argv[1] == 'MIS':
        work_done = True
        try:
          column_index = int(argv[2])
        except ValueError:
          print('Not a valid option')
          column_index = None
        
        if column_index !=None and column_index > -1 and column_index < len(original_columns) and column_index != id_column_index:
          column = original_columns[column_index]
          missing = column['missing_values']
          if len(missing) > 0:
            print('Dropping rows with missing values in ' + str(column_index) + ' [' + str(len(missing)) + ']')
            cursor.execute("DELETE FROM data WHERE id IN (" + ",".join(missing) + ")")
            database.commit()
            recalculate = True
          else:
            print('Nothing to do')
      elif argv[1] == 'VAL':
        work_done = True
        pass
    elif argv[0] == 'FILLM': # ----------------------------------------- FILL RIGHT / LEFT
      if argv[1] == 'RIGHT':
        if argv[2].find('-') > -1:
          fromcol = None
          tocol = None
          cols = argv[2].split('-')
          try:
            fromcol = int(cols[0])
            tocol = int(cols[1])
          except ValueError:
            print('Not a valid option')
        else:
          try:
            fromcol = int(argv[2])
            tocol = fromcol
          except ValueError:
            print('Not a valid option')
        
        if fromcol != None and tocol != None and fromcol > -1 and fromcol < len(original_columns) and tocol > -1 and tocol < len(original_columns):
          print("FILLING IN MISSING VALUES FROM THE RIGHT ...")
          cols = []
          for i in range(fromcol, tocol + 1):
            cols.append(original_columns[i]['name'])
          
          change = 0
          cursor.execute("SELECT [id],[" + "],[".join(cols) + "] FROM data")
          for row in cursor.fetchall():
            lastval = None
            update = []
            for i in range(len(row) - 1, 0, -1):
              field = row[i]
              if field != None and field != '':
                lastval = field
              else:
                if lastval != None:
                  update.append("[" + original_columns[i + fromcol - 1]['name'] + "]='" + str(lastval) + "'")
                  change += 1
            if len(update) > 0:
              sql = "UPDATE data SET " + ",".join(update) + " WHERE [id]='" + row[0] + "'"
              cursor.execute(sql)
              
          
          if change > 0:
            database.commit()
            print("... DONE " + str(change) + " VALUES ADDED")
            recalculate = True
          work_done = True
      elif argv[1] == 'LEFT':
        if argv[2].find('-') > -1:
          fromcol = None
          tocol = None
          cols = argv[2].split('-')
          try:
            fromcol = int(cols[0])
            tocol = int(cols[1])
          except ValueError:
            print('Not a valid option')
        else:
          try:
            fromcol = int(argv[2])
            tocol = fromcol
          except ValueError:
            print('Not a valid option')
        
        if fromcol != None and tocol != None and fromcol > -1 and fromcol < len(original_columns) and tocol > -1 and tocol < len(original_columns):
          print("FILLING IN MISSING VALUES FROM THE LEFT ...")
          cols = []
          for i in range(fromcol, tocol + 1):
            cols.append(original_columns[i]['name'])
          
          change = 0
          cursor.execute("SELECT [id],[" + "],[".join(cols) + "] FROM data")
          for row in cursor.fetchall():
            lastval = None
            update = []
            for i in range(1, len(row)):
              field = row[i]
              if field != None and field != '':
                lastval = field
              else:
                if lastval != None:
                  update.append("[" + original_columns[i + fromcol - 1]['name'] + "]='" + str(lastval) + "'")
                  change += 1
            if len(update) > 0:
              sql = "UPDATE data SET " + ",".join(update) + " WHERE [id]='" + row[0] + "'"
              cursor.execute(sql)
              
          
          if change > 0:
            database.commit()
            print("... DONE " + str(change) + " VALUES ADDED")
            recalculate = True
          work_done = True
        
  elif len(argv) == 4: #444444444444444444444444444444444444444444444444
    if argv[0] == 'MARK': # ----------------------------------------- MARK
      if argv[1] == 'COL':
        try:
          column_index = int(argv[2])
        except ValueError:
          print('Not a valid option')
          column_index == None
        
        if column_index != None and column_index > -1 and column_index < len(original_columns):
          value = oargv[3]
          if value in original_columns[column_index]['values'].keys():
            print('MARKING ALL ' + value + ' ...')
            for k in original_columns[column_index]['values'][value]:
              
              um = False
              ma = False
              if str(k) in unmarked:
                um = True
              if str(k) in marked:
                ma = True
              
              if not um and ma:
                pass
              elif um and not ma:
                unmarked.remove(str(k))
                marked.append(str(k))
              else:
                print("@ Error in marking: " + str(k) + " :: " + str(um) + "/" + str(ma))

            work_done = True
    elif argv[0] == 'SCALE': # ----------------------------------------- SCALE
      
      fromcol = None
      tocol = None
      if argv[1].find("-") > -1:
        cols = argv[1].split("-")
        try:
          fromcol = int(cols[0])
          tocol = int(cols[1])
        except ValueError:
          print('Not a valid option')
      else:
        try:
          fromcol = int(argv[1])
          tocol = fromcol
        except ValueError:
          print('Not a valid option')
      
      if fromcol != None and tocol != None and fromcol > -1 and tocol > -1 and fromcol < len(original_columns) and tocol < len(original_columns):

        if argv[2] == 'CLASS':
          try:
            column_index = int(argv[3])
          except ValueError:
            print('Not a valid option')
            column_index == None
          
          if column_index != None and column_index > -1 and column_index < len(original_columns):
            for classv in original_columns[column_index]['values'].keys():
              print('SCALING ALL ' + classv + ' BETWEEN 0 and 1 ...')
            
              maxval = None
              minval = None
              for scalecol in range(fromcol, tocol + 1):
                if original_columns[scalecol]['float']:
                  sql = "SELECT MAX([" + original_columns[scalecol]['name'] + "]),MIN([" + original_columns[scalecol]['name'] + "]) FROM data WHERE [" + original_columns[column_index]['name'] + "]='" + classv + "' AND [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
                  cursor.execute(sql)
                  row = cursor.fetchone()
                  if row[0] != None and (maxval == None or float(row[0]) > maxval):
                    maxval = float(row[0])
                
                  if row[1] != None and (minval == None or float(row[1]) < minval):
                    minval = float(row[1])

              
              if maxval != None and minval != None:
                
                print("... Found " + str(minval) + " - " + str(maxval))
                scaled = 0
                for scalecol in range(fromcol, tocol + 1):
                  if original_columns[scalecol]['float']:
                    if maxval != minval:
                      sql = "UPDATE data SET [" + original_columns[scalecol]['name'] + "]=([" + original_columns[scalecol]['name'] + "] - '" + str(minval) + "') / ('" + str(maxval) + "' - '" + str(minval) + "' ) WHERE [" + original_columns[column_index]['name'] + "]='" + classv + "' AND [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
                    else:
                      sql = "UPDATE data SET [" + original_columns[scalecol]['name'] + "]='0.5' WHERE [" + original_columns[column_index]['name'] + "]='" + classv + "' AND [" + original_columns[scalecol]['name'] + "] IS NOT NULL AND [" + original_columns[scalecol]['name'] + "] != ''"
                  
                    #print(sql)
                    cursor.execute(sql)
                    recalculate = True
                    scaled += 1
                print("... scaled " + str(scaled) + " columns")
              else:
                print("... Nothing todo")
            if recalculate:
              database.commit()

            print('DONE')
    elif argv[0] == 'FILLM':
      if argv[1] == 'CMEAN':
        try:
          column_index = int(argv[2])
        except ValueError:
          print('Not a valid option')
          column_index == None
        
        if column_index > -1 and column_index < len(original_columns):
          fromcol = None
          tocol = None
          if argv[3].find("-") > -1:
            cols = argv[3].split("-")
            try:
              fromcol = int(cols[0])
              tocol = int(cols[1])
            except ValueError:
              print('Not a valid option')
          else:
            try:
              fromcol = int(argv[3])
              tocol = fromcol
            except ValueError:
              print('Not a valid option')
          
          if fromcol != None and tocol != None and fromcol > -1 and tocol > -1 and fromcol < len(original_columns) and tocol < len(original_columns):
            means = {}
            print('REPLACING MISSING VALUES WITH CLASS AVERAGES ...')
            cols = []
            for i in range(fromcol, tocol + 1):
              cols.append(original_columns[i]['name'])
            cursor.execute("SELECT [id],[" + original_columns[column_index]['name'] + "],[" + "],[".join(cols) + "] FROM data")
            change = 0
            for row in cursor.fetchall():
              update = []
              for i in range(2, len(row)):
                if row[i] == None or row[i] == '':
                  key = row[1] + ":" + str(i)
                  if key not in means:
                    cursor.execute("SELECT AVG([" + original_columns[i + fromcol - 2]['name'] + "]) FROM data WHERE [" + original_columns[column_index]['name'] + "]='" + row[1] + "'")
                    avg = cursor.fetchone()[0]
                    means[key] = avg
                  else:
                    avg = means[key]
                  change += 1
                  update.append( "[" + original_columns[i + fromcol - 2]['name'] + "]='" + str(avg) + "'")
              if len(update) > 0:
                cursor.execute("UPDATE data SET " + ",".join(update) + " WHERE [id]='" + row[0] + "'")
            
            if change > 0:
              database.commit()
              recalculate = True
            print('... DONE ' + str(change) + ' VALUES FILLED IN')
            work_done = True

  # -------------------------------------------------------------------- Recalculate statistics
  if recalculate:
    print("Recalculating statistics ...")
    database.rollback()
    for i, column in enumerate(original_columns):
      column = {'float':True, 'name':column['name'], 'max':None, 'min':None, 'missing_values':[], 'mean':0.0, 'values':{}}
      original_columns[i] = column
    
    i = 0
    for row in cursor.execute("SELECT * FROM data"):
    #for i in range(original_data_count):
    #  row = original_data[i]
      
      for j in range(len(row)):
        field = row[j]
        column = original_columns[j]
    
        if field == None or field == '':
          column['missing_values'].append(i)
        else:
          if column['float']:
            
            try:
              field = float(field)
              # Collect statistics about the data
              if column['min'] == None or field < column['min']:
                column['min'] = field
              if column['max'] == None or field > column['max']:
                column['max'] = field
              column['mean'] += field
            
            except ValueError:
              column['float'] = False
              column['mean'] = 0
              pass
        
          if str(field) not in column['values']:
            column['values'][str(field)] = []
          column['values'][str(field)].append(i)
      i += 1
    cursor.execute("SELECT COUNT(*) FROM data")
    original_data_count = cursor.fetchone()[0]

    # Compute the mean and output the statistics
    for i in range(len(original_columns)):
      column = original_columns[i]
      if column['float']:
        if original_data_count - len(column['missing_values']) > 0:
          column['mean'] = column['mean'] / (original_data_count - len(column['missing_values']))
        else:
          column['mean'] = 0
  
  if len(unmarked) + len(marked) != original_data_count:
    print('Checking marked data ...')
    for k in marked:
      if str(k) in unmarked:
        unmarked.remove(str(k))
    for k in unmarked:
      if str(k) in marked:
        unmarked.remove(str(k))
    
    cursor.execute("SELECT id FROM data")
    for row in cursor.fetchall():
      if row[0] not in marked and row[0] not in unmarked:
        unmarked.append(row[0])

  #--------------------------------------------------------------------- No work was done - display help
  if not work_done:
    print('Available Commands:')
    print("sql SQL_STATEMENT \t - Execute an sql statement")
    print("rec \t - Recalculate statistics (you should not need this)")
    print("drop")
    print("\tmis COLUMN_INDEX \t - Drop all rows with missing values in this column")
    print("\tcol COLUMN_INDEX \t - Drop the specified column altogether")
    print("\tmark \t - Drop all marked entries")
    print("\tunmark \t - Drop all unmarked entries")
    print('mark')
    print("\tcol COLUMN_INDEX VALUE \t - Mark all entries with the specified value in column")
    print("minv\t - Inverse marked entries")
    print("mres\t - Reset marks")
    print("mshow\t - Show all marked")
    print("mushow\t - Show all unmarked")
    print("scale COLUMN_INDEX[-TO_COLUMN_INDEX] [class COLUMN_INDEX] \t - scale the values between 0 and 1 but only within the same class")
    print("fillm (right | left | classmean COLUMN_INDEX) FROM_COL[-TO_COL]")
    print("values COLUMN_INDEX \t - Show all possible values and their distribution for a column")
    print('quit/exit\t - Exit program without saving')
