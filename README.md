# Hadoop-Reversed-Index
 
### Simple inverted indexing algorithm implemented with Hadoop

- An inversed index contains for each distinct unique word, a list of files containing the given word with its location within the file (line number).
- The algorithm is designed to run on a local Hadoop cluster and performs a series of MapReduce jobs using an HDFS instance for input and output. 
