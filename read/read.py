# Function readFromPath
# Checks if path is a file or directory
# If directory, finds first file in directory with matching prefix, error if no prefix
# First file found or if path is a file
#   Do best guess encoding, otherwise raise error use chardet library
#   Read first 500 lines from file using delim argument, filepath, and encoding
#   Foreach column
#     Find first value
#     Do best guess as to data type
#   build create table statement using args schema and tablename and results of above
# return create table string
