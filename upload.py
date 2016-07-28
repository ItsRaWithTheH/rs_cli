from dotenv import load_dotenv
from os.path import join, dirname
import os
import sys
import boto3
import threading


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
          sys.stdout.flush()

def find(name, path):
  for root, dirs, files in os.walk(path):
    if name in files:
      return os.path.join(root, name)
#
# loads dotenv
dotenv_path = os.path.abspath('./.env')
if os.path.exists(dotenv_path) == False:
  print('Cannot locate .env file using path --> {}'.format(dotenv_path), file=sys.stderr)
  sys.exit(1)
else:
  load_dotenv(dotenv_path)

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
        s3_client.upload_file(absolutePath, aws_config["s3_bucket"], tablename + '_' + str(i))
        upload.append('s3://{bucket}/{path}'.format(bucket=aws_config["s3_bucket"], path=tablename + '_' + str(i)))
        i = i + 1
  elif os.path.exists(filePath):
    s3_client.upload_file(filePath, aws_config["s3_bucket"], tablename+'_0', None, ProgressPercentage(filePath))
    upload.append('s3://{bucket}/{path}'.format(bucket=aws_config["s3_bucket"], path=tablename + '_0'))

  else:
    raise Error("Path is no good")

  return upload

aws_config = {
  'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
  'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
  'aws_default_region': os.environ.get('AWS_DEFAULT_REGION'),
  's3_bucket': os.environ.get('S3_BUCKET')
}

path = os.path.abspath('./test2.csv')

uploadToS3(path, None, aws_config, 'testCSV')