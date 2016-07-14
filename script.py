from datetime import date, datetime
import os
import re
import csv
import pandas as pd

def get_column_datatype(cell):
  """
  Gets the Redshift datatype for a column in the csv

  type cell: can be one of the following types:
      int, decimal, float,
  """

  # subfunction checking if a cell is a float or not
  def _isfloat(s):
    try:
      float(s)
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
    if len(s) == 9:
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
    return 'date'
  elif cell.isdigit():
    return 'integer'
  elif _isfloat(cell):
    return 'real'
  elif _isbool(cell):
    return 'bool'
  else:
    return 'varchar(256)'


def load_csv_to_df(csv_path):
  return pd.read_csv(csv_path, parse_dates=True, infer_datetime_format=True)

def get_create_table(df):
  print(df)
  headers = df.keys()


