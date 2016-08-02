#RUN PIP INSTALL PYEXCEL
#RUN PIP INSTALL PYEXCEL_XLSX

import pyexcel as pe
import collections

def get_rs_dtypes(xlsx_file):
  """
  Reads some excel file that has been created from running write_to_spreadsheet.py
  and that the user has manually written in datatypes for the column names whose rs datatypes
  were guessed incorrectly

  Output should be a list of strings (each string is 'column_name redshift_datatype')
  """

  records = pe.get_records(file_name = xlsx_file+".xlsx")
  column_names_list = list()
  datatypes_list = list()
  for record in records:
    column_names_list.append(record['Column Name'])
    if record['User Input Datatype']=='':
        datatypes_list.append('VARCHAR(256)')
    else:
        datatypes_list.append(record['User Input Datatype'])

    names_and_types_list = list()
    for i in range(0, len(datatypes_list)):
      names_and_types_list.append(column_names_list[i] + " " + datatypes_list[i])

  return names_and_types_list




