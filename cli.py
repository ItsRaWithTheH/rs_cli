# -*- coding: utf-8 -*-
""" CLI tool for creating a table in redshift
and populating the table with data from a csv."""

from dotenv import load_dotenv
from os.path import join, dirname
import argparse
import os
import sys

import query_script
import redshift as db
import pandas as pd
import script as cliTools


# loads dotenv
dotenv_path = join(dirname(__file__), '.env')
if os.path.exists(dotenv_path) == False:
  print('Cannot locate .env file using path --> {}'.format(dotenv_path), file=sys.stderr)
  sys.exit(1)
else:
  load_dotenv(dotenv_path)

HELP_STRING = """To run, call 'python cli.py'
  followed by an input file other options"""

def verify_aws_config(aws_config, command_name):
  """Verify the aws configuration has the proper parameters"""
  if aws_config['s3_bucket'] is None:
    raise RuntimeError('must supply s3bucket arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_access_key_id'] is None:
    raise RuntimeError('must supply access arg to run {cmd} command.'.format(cmd=command_name))
  if aws_config['aws_secret_access_key'] is None:
    raise RuntimeError('must supply secret arg to run {cmd} command.'.format(cmd=command_name))
  return

def verify_db_config(db_config):
  """Verify the database config has the proper parameters"""
  if db_config['user'] is None:
    raise RuntimeError('must supply user arg to run a cli command.')
  if db_config['pwd'] is None:
    raise RuntimeError('must supply pwd arg to run a cli command')

#access AWS credentials by reading from .env file
parser = argparse.ArgumentParser(description='Pass data from CSV to Redshift')
parser.add_argument('input', type=str, required=True, help='file or directory to upload to redshift')
parser.add_argument('--table_name', type=str, required=True, help='destination table name')
parser.add_argument('--schema', type=str, default=os.environ.get('DATABASE_SCHEMA'),
                    help='the schema where tables should upload to')
parser.add_argument('--delimeter', type=str, default=',', help='delimeter to use')
parser.add_argument('--prefix', type=str, help='prefix for files when path is a directory')

args = parser.parse_args()


db_config = {
    'dbname': args.db_name,
    'user': args.db_user,
    'pwd': args.db_pwd,
    'host': args.db_host,
    'port': args.db_port,
    'dialect_and_driver': args.db_dialect_driver
}
aws_config = {
    'aws_access_key_id': args.access,
    'aws_secret_access_key': args.secret,
    'aws_default_region': args.awsRegion,
    's3_bucket': args.s3bucket
}

verify_db_config(db_config)

# initial error checking
if (args.input IS NONE):
  raise;

# Call read package function using the file path, return create table string
# Call write package run query with the create table string
# Next call write package function to upload to s3 given the filepath, return the path(s) on s3
# Foreach s3 path, call write package function to run a COPY statement on redshift

