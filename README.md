# Redshift CLI

This python script can be used as a command line tool, it has actions to run (currently only csv)


## Setup
Must be running Python 3.5+

- clone this repository, go into project root directory

- run `pip install -r requirements.txt` to get dependencies

- copy environment template into project root

  Unix/Linux
  `cp env/template .env`

  Win
  `copy env/template .env`

## How to use
run `python cli.py -h` to see all options

## Help

Pass data from CSV to Redshift

positional arguments: (these are REQUIRED)
  action                either "create_excel" or "load_excel"
  input                 file or directory to upload to redshift

optional arguments:
  -h, --help            show this help message and exit
  --excel_sheet EXCEL_SHEET
                        path to excel sheet to be created/loaded
  --table_name TABLE_NAME
                        destination table name
  --schema SCHEMA       the schema where tables should upload to
  --delimiter DELIMITER
                        delimeter to use
  --prefix PREFIX       prefix for files when path is a directory. If only
                        looking for one file please just pass a valid prefix
                        (could just be the first couple letters of the file
                        name that specifies that file in the directory) for
                        that file

###  CREATE EXCEL SPREADSHEET TO MANUALLY INPUT REDSHIFT DATATYPES FOR THE COLUMN NAMES

run "python cli.py create_excel 'ABSOLUTE PATH TO FILE/DIRECTORY TO UPLOAD TO RS' --excel_sheet='NAME FOR EXCEL SPREADSHEET DO NOT INCLUDE .xlsx EXTENSION' 
--delimiter='DELIMITER FOR INPUT FILE. If delimiter is a comma no need to include this flag'""

Where create_excel is your action and the 'ABSOLUTE PATH TO FILE/DIRECTORY' is your input
--excel_sheet is a mandatory flag when calling the create_excel action

After you run this command, you should find your excel file with the --excel_sheet name you gave in youor rs-cli directory.
Open up the file and manually fill out the 'USER INPUT' column with the Redshift datatypes for the respective column names

### LOAD EXCEL SPREADSHEET AND UPLOAD DATA FROM INPUT FILE TO REDSHIFT

run "python cli.py load_excel 'ABSOLUTE PATH TO FILE/DIRECTORY TO UPLOAD TO RS' --excel_sheet='NAME FOR EXCEL SPREADSHEET DO NOT INCLUDE .xlsx EXTENSION' 
--delimiter='DELIMITER FOR INPUT FILE. If delimiter is a comma no need to include this flag' --table_name='TABLE NAME IN RS' --schema='SCHEMA WHERE TABLE WILL BE CREATED IN RS'"

Where load_excel is your action and the 'ABSOLUTE PATH TO FILE/DIRECTORY' is your input
--excel_sheet, --table_name, and --schema are a mandatory flags when calling the load_excel action

After you run this command, you should find your table containing the data from the INPUT file in RS in the schema you specified with the table name you specified.
The columns of the table you should of those specified in the 'USER INPUT' column of the excel spreadsheet you specified with the --excel_sheet flag.

ENJOY!


