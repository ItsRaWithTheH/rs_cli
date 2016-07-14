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
  `copy env\template .env`
  
- Fill in all values in `.env` with your personal environment

## How to use
run `python cli.py csv -h` to see all options

### CSV

run `python cli.py csv --input="" --table_name=""`

Where `input` is a path to a csv file, and `table_name` is the name of the table on redshift to upload

