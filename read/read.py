# Function readFromPath
# Checks if path is a file or directory
# If directory, finds first file in directory with matching prefix, RuntimeError if no prefix
# First file found or if path is a file
#   Do best guess encoding, otherwise raise RuntimeError use chardet library
#   Read first 500 lines from file using delim argument, filepath, and encoding
#   Foreach column
#     Find first value
#     Do best guess as to data type
#   build create table statement using args schema and tablename and results of above
# return create table string

# import urllib
import os
from chardet.universaldetector import UniversalDetector
import csv
import collections
import re

def readFromPath (path, delimeter, prefix, schema, tablename):
  #Check if it is directory
  absolutePath = os.path.abspath(os.path.expanduser(path))
  if os.path.isdir(absolutePath):
    if prefix is None:
      raise RuntimeError("must specify a prefix if passing in a directory")

    for file in os.listdir(path):
      if file.startswith(prefix) and ('.txt' or '.csv' in file):
        absolutePath = absolutePath + '/' + file
        break
    else:
      raise RuntimeError("No files found with prefix")
  elif not os.path.exists(absolutePath):
    raise RuntimeError("File not Found")
  if absolutePath is not None:
    # guessed_encoding = _bestGuessEncoding(absolutePath)
    dataHead = _readFile(absolutePath, delimeter)
    return _getQuery(dataHead, schema, tablename)
  else:
    raise RuntimeError("Unknown Error")



def _readFile(filePath, delim, guessed_encoding=None):
  with open(filePath, encoding=guessed_encoding) as infile:
    dialect = csv.Sniffer().sniff(infile.read(1024))
    rows = csv.reader(infile, delimiter=delim)
    print(rows)
    readData = list()
    maxRows = 500
    # headers = next(rows)
    # print(headers)
    for i, element in enumerate(rows):
      if i < maxRows:
        readData.append(element)
  return readData

def _bestGuessEncoding(filePath):
  """
    find the best guess encoding for the file given the filePath
    (filePath includes the file in it)
  """
  # rawdata = open(filePath, 'rb').read()
  rawdata = open(filePath, 'rb')
  detector = UniversalDetector()
  for line in rawdata.readlines():
    detector.feed(line)
    if detector.done: break
  detector.close()
  rawdata.close()
  confidence = detector.result['confidence']
  encoding = detector.result['encoding']

  if confidence < 0.75:
    raise RuntimeError("Encoding looks a little strange. Confidence is below 75%. Make changes to file")

  return encoding

def _getQuery (readData, schema, tablename):
  headers = readData.pop(0)

  tableDef = list()
  for header in headers:
    tableDef.append({
      "name": header,
      "type": None
    })

  for i in range(0, len(readData)):
    row = readData[i]
    for j in range(0, len(row)):
      cell = row[j]
      columnDef = tableDef[j]
      if columnDef['type'] is None:
        columnDef['type'] = get_column_datatype(cell)

  createStr = 'CREATE TABLE IF NOT EXISTS {schema}.{tablename} ('.format(schema=schema, tablename=tablename)
  for column in tableDef:
    createStr += '\n{name} {type},'.format(name=column['name'], type=column['type'])
  createStr = createStr.rstrip(',')
  createStr +=  '\n);'
  return createStr;


def get_column_datatype(cell):
  """
  Gets the Redshift datatype for a column in the csv

  type cell: can be one of the following types:
      int, decimal, float,
  """

  # subfunction checking if a cell is a float or not
  def _isfloat(s):
    if re.search('.*E0.*', s) is not None:
      return False
    try:
      float(s)
      return True
    except ValueError:
      return False

  def _isint(s):
    try:
      int(s)
      return True
    except ValueError:
      return False

  # subfunction checking if a cell is a bool or not
  # NOTE: if a cell has a bool datatype then it must be
  #      initially recorded with capital 'T' for 'True' or capital 'F' for 'False'
  def _isbool(s):
    if s == 'True' or s == 'False':
      return True
    else:
      return False

  # subfunction checking if a cell is a date or not
  # NOTE: this subfunction only accounts for two different date formats
  #      from the CSV.
  #      Format One: 8 digits, where the first 4
  #          digits represent year, then next 2 digits represent month, and
  #          final two digits represent day
  #      Format Two: 2 digits for day of the day followed by
  #          string of 3 letters representing month (first 3 letters of month name)
  #          followed by 4 digits for year
  def _isdate(s):
    # Format One check
    if _isint(s) and len(s) == 8:
      potential_year = int(s[:4].lstrip('0'))
      potential_month = int(s[4:6].lstrip('0'))
      potential_day = int(s[6:].lstrip('0'))
      if potential_year in range(0, 2016) and potential_month in range(1, 12) \
          and potential_day in range(1, 31):
        return True
        # Format Two check
    if len(s) == 9 and re.search('\d{2}[a-zA-z]{3}\d{4}', s) is not None:
      potential_year2 = int(s[5:].lstrip('0'))
      potential_day2 = int(s[:2].lstrip('0'))
      # checking if 3 letters in the middle of the string represent a valid month
      if s[2:5] in ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', \
                    'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC') and potential_year2 in range(0, 2016) \
          and potential_day2 in range(1, 31):
        return True
    # if neither Format One of Format Two
    return False

  # strip of all white space before anf after the string
  cell = cell.strip(' ')
  if _isdate(cell):
    return 'DATE'
  elif _isint(cell):
    return 'REAL'
  elif _isfloat(cell):
    return 'REAL'
  elif _isbool(cell):
    return 'BOOLEAN'
  else:
    return 'VARCHAR(256)'
