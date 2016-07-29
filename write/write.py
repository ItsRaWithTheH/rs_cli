# Upload to s3
# Takes in file path or directory path, aws_config, prefix and tablename
# Create folder on s3 bucket that matches tablename
# If file, upload file
# If directory, upload all files under prefix
# Return a List of paths to uploaded files

# Perform SQL Query
# takes in db_config, sql_string
# Creates connection to DB
# Runs query, returns result

# Load DB
# Takes in List of s3_paths, db_config, aws_config, tablename, schema
# Creates a set of COPY statements
# Calls perform SQL query function for each one
# Return error or result(s)

import boto3
import os
import redshift as db
import sys
import threading

REDSHIFT_COPY_CSV = """COPY {schema}.{tablename}
  FROM '{s3Path}'
  credentials 'aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}'
  EMPTYASNULL
  FILLRECORD
  CSV
  DELIMITER AS '{delimiter}'
  IGNOREHEADER AS 1
  dateformat AS 'auto';"""

class ProgressPercentage(object):
  def __init__(self, filename):
      self._filename = filename
      self._size = float(os.path.getsize(filename))
      self._seen_so_far = 0
      self._lock = threading.Lock()

  def __call__(self, bytes_amount):
      # To simplify we'll assume this is hooked up
      # to a single filename.
      with self._lock:
          self._seen_so_far += bytes_amount
          percentage = (self._seen_so_far / self._size) * 100
          sys.stdout.write(
              "\r%s  %s / %s  (%.2f%%)" % (
                  self._filename, self._seen_so_far, self._size,
                  percentage))
          if percentage != 100:
              sys.stdout.flush()

def executeSQL(query, db_config):
  conn = db.create_sql_conn(db_config)
  cursor = conn.cursor()
  result = cursor.execute(query)
  conn.commit()
  cursor.close()
  conn.close()
  return result

def loadDB(s3Path, db_config, aws_config, tablename, schema, delim=','):
  conn = db.create_sql_conn(db_config)
  cursor = conn.cursor()

  copy_statement = REDSHIFT_COPY_CSV.format(
    schema=schema,
    tablename=tablename,
    s3Path=s3Path,
    access_key_id=aws_config['aws_access_key_id'],
    secret_access_key=aws_config['aws_secret_access_key'],
    delimiter=delim
  )

  cursor.execute(copy_statement)
  conn.commit()
  cursor.close()
  conn.close()
  return

def uploadToS3(path, prefix, aws_config, tablename):
  access_key_id = aws_config['aws_access_key_id']
  secret_access_key = aws_config['aws_secret_access_key']
  s3_client = boto3.client(
    's3',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key,
  )
  filePath = os.path.abspath(path)
  upload = list()
  if os.path.isdir(filePath):
    if prefix is None:
      raise Error("must specify a prefix if passing in a directory")
    i = 0;
    for file in os.listdir(path):
      if file.startswith(prefix) and ('.txt' or '.csv' in file):
        absolutePath = os.path.abspath(path) + '/' + file
        s3_client.upload_file(absolutePath, aws_config["s3_bucket"], tablename + '_' + str(i), None, ProgressPercentage(filePath))
        upload.append('s3://{bucket}/{path}'.format(bucket=aws_config["s3_bucket"], path=tablename + '_' + str(i)))
        i = i + 1
  elif os.path.exists(filePath):
    s3_client.upload_file(filePath, aws_config["s3_bucket"], tablename+'_0', None, ProgressPercentage(filePath))
    upload.append('s3://{bucket}/{path}'.format(bucket=aws_config["s3_bucket"], path=tablename + '_0'))

  else:
    raise Error("Path is no good")

  return upload
