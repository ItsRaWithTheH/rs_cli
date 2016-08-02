#import xlwt

import os
from chardet.universaldetector import UniversalDetector
import csv
import collections
import re
#RUN PIP INSTALL XLSXWRITER
import xlsxwriter

def readFromPath (path, delimiter, prefix, schema, tablename):
  #Check if it is directory
  if os.path.isdir(path):
    if prefix is None:
      raise Error("must specify a prefix if passing in a directory")

    for file in os.listdir(path):
      if file.startswith(prefix) and ('.txt' or '.csv' in file):
        absolutePath = os.path.abspath(path) + '/' + file
        guessed_encoding = _bestGuessEncoding(absolutePath)
        break
    else:
      raise Error("No files found with prefix")
  elif os.path.exists(path):
    absolutePath = os.path.abspath(path)
    guessed_encoding = _bestGuessEncoding(absolutePath)
  if absolutePath is not None:
    dataHead = _readFile(absolutePath, delimiter, guessed_encoding)
    tableDef = _getColumnDtypePairs(dataHead)
    return to_spreadsheet(tableDef[0], tableDef[1])
  else:
    raise Error("File not found")

def _readFile(filePath, delim, guessed_encoding):
  with open(filePath, encoding=guessed_encoding) as infile:
    rows = csv.reader(infile, delimiter=delim)
    readData = list()
    maxRows = 500
    for i, element in enumerate(rows):
      if i < maxRows:
        readData.append(element)
  return readData
  #readData is a list of lists

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
    raise Error("Encoding looks a little strange. Confidence is below 75%. Make changes to file")

  return encoding

def _getColumnDtypePairs (readData):
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

  #return tableDef
  column_names_list = list()
  datatype_list = list()
  for name_type_pair in tableDef:
    column_names_list.append(name_type_pair['name'])
    datatype_list.append(name_type_pair['type'])

  return column_names_list, datatype_list

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
  #      Format Two: 2 digits for day of the month followed by
  #          string of 3 letters representing month (first 3 letters of month name)
  #          followed by 4 digits for year
  def _isdate(s):
    # Format One check
    if s.isdigit() and len(s) == 8:
      potential_year = int(s[:4].lstrip('0'))
      potential_month = int(s[4:6].lstrip('0'))
      potential_day = int(s[6:].lstrip('0'))
      if potential_year in range(0, 2016) and potential_month in range(1, 12) \
          and potential_day in range(1, 31):
        return True
        # Format Two check
    if len(s) == 9 and s[5:].isdigit():
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


def to_spreadsheet(column_names_list, guessed_datatype_list, spreadsheet_name):

  #book = xlwt.Workbook(encoding=guessed_encoding)
  workbook = xlsxwriter.Workbook(spreadsheet_name+".xlsx")

  #sheet1 = book.add_sheet("Input Redshift Datatype Spreadsheet")
  worksheet = workbook.add_worksheet()

  worksheet.write(0, 0, "Column Name")
  worksheet.write(0, 1, "Guessed Datatype")
  worksheet.write(0, 2, "User Input Datatype")

  def _list_to_excel_column(list_of_entries, column_to_write_to):
    """
    list_of_entries: list of entries to want to write into individual cells for a column in the excel sheet
      possible arguments are column_names_list and guessed_datatype_list
    """
    i = 1
    for n in list_of_entries:
      worksheet.write(i, column_to_write_to, n)
      i = i + 1

  _list_to_excel_column(column_names_list, 0)
  _list_to_excel_column(guessed_datatype_list, 1)

  #book.save(spreadsheet_name+".xls")



