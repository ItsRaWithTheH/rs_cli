# -*- coding: utf-8 -*-
""" CLI tool for creating a table in redshift
and populating the table with data from a csv."""

from dotenv import load_dotenv
from os.path import join, dirname
import argparse
import os
import sys

from read import read
from write import write

def find(name, path):
  for root, dirs, files in os.walk(path):
    if name in files:
      return os.path.join(root, name)
#
# loads dotenv
dotenv_path = find('.env', '/Users/rmahida/Projects/personal/rs_cli')
if os.path.exists(dotenv_path) == False:
  print('Cannot locate .env file using path --> {}'.format(dotenv_path), file=sys.stderr)
  sys.exit(1)
else:
  load_dotenv(dotenv_path)
#
HELP_STRING = """To run, call 'python cli.py'
  followed by an input file other options"""
#
def verify_aws_config(aws_config, command_name):
  """Verify the aws configuration has the proper parameters"""
  if aws_config['s3_bucket'] is None:
    raise RuntimeError('must supply s3bucket arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_access_key_id'] is None:
    raise RuntimeError('must supply access arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_secret_access_key'] is None:
    raise RuntimeError('must supply secret arg to run {cmd} command.'.format(cmd=command_name))
  return
#
def verify_db_config(db_config):
  """Verify the database config has the proper parameters"""
  if db_config['user'] is None:
    raise RuntimeError('must supply user arg to run a cli command.')
  if db_config['pwd'] is None:
    raise RuntimeError('must supply pwd arg to run a cli command')

#access AWS credentials by reading from .env file
parser = argparse.ArgumentParser(description='Pass data from CSV to Redshift')
parser.add_argument('input', type=str, help='file or directory to upload to redshift')
parser.add_argument('--table_name', type=str, required=True, help='destination table name')
parser.add_argument('--schema', type=str, default=os.environ.get('DATABASE_SCHEMA'),
                    help='the schema where tables should upload to')
parser.add_argument('--delimiter', type=str, default=',', help='delimeter to use')
parser.add_argument('--prefix', type=str, \
  help="""prefix for files when path is a directory. If only looking for one file
  please just pass a valid prefix (could just be the first couple letters of the file
  name that specifies that file in the directory) for that file""")

args = parser.parse_args()


db_config = {
  'dbname': os.environ.get('DATABASE_NAME'),
  'user': os.environ.get('DATABASE_USR'),
  'pwd': os.environ.get('DATABASE_PWD'),
  'host': os.environ.get('DATABASE_HOST'),
  'port': os.environ.get('DATABASE_PORT'),
  'dialect_and_driver': os.environ.get('DATABASE_DIALECT_DRIVER')
}
aws_config = {
  'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
  'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
  'aws_default_region': os.environ.get('AWS_DEFAULT_REGION'),
  's3_bucket': os.environ.get('S3_BUCKET')
}

verify_db_config(db_config)

# initial error checking
if (args.input == None):
  raise RuntimeError("Invalid argument passed into commandline.")
else:
  # Call read package function using the file path, return create table string
  createQuery = read.readFromPath(args.input, args.delimiter, args.prefix, args.schema, args.table_name)
  # Call write package run query with the create table string
  if createQuery is None:
    raise Error("oops")
  createTable = write.executeSQL(createQuery, db_config)
  # Next call write package function to upload to s3 given the filepath, return the path(s) on s3
  uploadFiles = write.uploadToS3(args.input, args.prefix, aws_config, args.table_name)
  # Foreach s3 path, call write package function to run a COPY statement on redshift
  for s3Path in uploadFiles:
    write.loadDB(s3Path, db_config, aws_config, args.table_name, args.schema)

