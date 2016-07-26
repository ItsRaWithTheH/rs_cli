# Function readFromPath
import os
import sys
import glob
import chardet
import pandas as pd
import csv
import collections

sys.path.insert(0, '/Users/jlee/Desktop/CLI')
import query_script

def readFromPath(argPath, argPrefix, argDelim, argSchema, argTablename):
  """
  Checks if path is a file or directory
  If it's a directory, readFromPath finds first file in directory
  with matching prefix, and throws an error if no occurrence of the prefix specified

  When the first file is found or if the path is a file:
  Do best guess encoding, otherwise raise error use chardet library
  Read first 500 lines from file using delimiter argument, filepath, and encoding
  Then for each column: 
    Find first value (non-empty cell)
    Do best guess as to what data type it is
  Build create table statement using args schema and tablename and results of above
  Return create table string
  """

  # Checks if path is a file or directory
  if os.path.isdir(argPath):
    # Path goes to directory. Find first file in
    # directory with matching prefix
    for file in os.listdir(argPath):
      if file.startswith(argPrefix) and ('.txt' or '.csv' in file):
        absolute_file_path = argPath + file if argPath[-1] == '/' \
          else argPath + '/' + file 
        guessed_encoding = _bestGuessEncoding(absolute_file_path)
        with open(absolute_file_path, encoding=guessed_encoding) as infile:
          rows = csv.reader(infile, delimiter=argDelim)
          headers = next(rows)
          #types = {header: None for header in headers}
          types = collections.OrderedDict.fromkeys(headers)

          for row in rows:
            for i in range(0, len(row)):
              if row[i] != '':
                types[headers[i]] = query_script.get_column_datatype(row[i])
              else:
                pass

          for header in headers:
            if types[header] == None:
              types[header] = 'varchar(256)'
          return _getQuery(types, argSchema, argTablename)

    raise StandardError("no file in directory had the expected prefix.")
  
  else:
    # Path does not go to directory
    if os.path.exists(argPath):
      # Path goes to a file
      guessed_encoding = _bestGuessEncoding(argPath)
      with open(argPath, encoding=guessed_encoding) as infile:
        rows = csv.reader(infile, delimiter=argDelim)
        headers = next(rows)
        types = {header: None for header in headers}

        print(types)

        for row in rows:
          for i in range(0, len(row)):
            if row[i] != '':
              types[headers[i]] = query_script.get_column_datatype(row[i])
            else:
              pass

        for header in headers:
          if types[header] == None:
            types[header] = 'varchar(256)'
        return _getQuery(types, argSchema, argTablename)

    else:
      # Path neither goes to a file nor directory
      raise IOError('path goes to neither a valid file nor directory.')
                        
                
def _bestGuessEncoding(filePath):
  """
    find the best guess encoding for the file given the filePath
    (filePath includes the file in it)
  """
  rawdata = open(filePath, 'rb').read()
  confidence = chardet.detect(rawdata)['confidence']
  encoding = chardet.detect(rawdata)['encoding']

  if confidence < 0.75:
    raise Error("Encoding looks a little strange. Confidence is below 75%. Make changes to file")

  return encoding


def _getQuery(some_ordered_dict, argSchema, argTablename):
  """
  some_ordered_dict: a collections.OrderedDict whose keys are the column names
      and whose values are the respective column's Redshift datatype
  """
  column_headers_and_types = ', '.join([col_name + " " + data_type for col_name, data_type in some_ordered_dict.items()])
  final_query = "CREATE TABLE IF NOT EXISTS " + argSchema + "." + argTablename + " (" + \
      column_headers_and_types + ")"
  
  #removes quotation marks around header names for the query
  for char in final_query:
      final_query = final_query.replace("'", "")
  
  #print(csv_rows_list)
  
  #print(final_query)
  return final_query

########################
# CREATE THE DATAFRAME #
########################

def getDfFromListOfCsvs(argPath, argPrefix, argDelim):
  """
  raises an error if the files do not have the same column names /
  column datatypes or if the delimiters for the files differ from one another

  list_to_create_df_from : list of files to create df from
  argDelim: delimiter to parse csv/txt with
  """
  list_to_create_df_from = _getListToCreateDfFrom(argPath, argPrefix)

  accumulated_df = pd.DataFrame()
  for file in list_to_create_df_from:
    df = pd.read_csv(file, delimiter=argDelim)
    accumulated_df = accumulated_df.append(df)
  accumulated_df = accumulated_df.reset_index(drop=True)

  return accumulated_df

def _getListToCreateDfFrom(argPath, argPrefix):
  """
  argPath : path to directory/file where the csv/txt file(s) of
    interest are
  argPrefix: prefix of the file name(s) you are interested in
  """
  path_list = []
  if os.path.isdir(argPath):
    for file in os.listdir(argPath):
      if file.startswith(argPrefix) and ('.txt' or '.csv' in file):
        absolute_file_path = argPath + file if argPath[-1] == '/' \
          else argPath + '/' + file 
        path_list.append(absolute_file_path)

    if path_list == []:
      raise Error("The path and prefix you offered returned no valid files")
    return path_list

  else:
    if os.path.exists(argPath):
      path_list = [argPath]
      return path_list

    else:
      # Path neither goes to a file nor directory
      raise IOError('path goes to neither a valid file nor directory.')



