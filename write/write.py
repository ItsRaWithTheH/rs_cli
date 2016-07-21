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
