import sys
import os
import math
# from test_helper import Test
from operator import add, div

#--------- Spark setup -----------
# Configure the environment for PySpark
if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/opt/spark'
# Create a variable for our root path
SPARK_HOME = os.environ['SPARK_HOME']
# Add the PySpark/py4j to the Python Path
sys.path.insert(0, os.path.join(SPARK_HOME, "python", "lib"))
sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
from pyspark.sql import Row
from pyspark import SparkContext
sc = SparkContext("local", "Lab3")
#--------- Spark setup -----------

# ------ DAY 1 -------
ut = sc.textFile('data/uber/Uber-Jan-Feb-FOIL.csv')
rows = ut.map(lambda  line: line.split(','))

rows.map(lambda row: row[0]).distinct().count()

rows.map(lambda row: row[0]).distinct().collect()

rows.filter(lambda  row: 'B02617' in row).count()

base02617 = rows.filter(lambda  row: 'B02617' in row)

base02617.filter(lambda row: int(row[3]) > 15000).count()

base02617.filter(lambda row: int(row[3]) > 15000).map(lambda day: day[1]).distinct().count()

filteredRows = (sc.textFile('data/uber/Uber-Jan-Feb-FOIL.csv')
                .filter(lambda line: 'base' not in line)
                .map(lambda line: line.split(',')) )

filteredRows.map(lambda kp: (kp[0], int(kp[3]))).reduceByKey(add).collect()

filteredRows.map(lambda kp: (kp[0], int(kp[3]))).reduceByKey(add).takeOrdered(10, key=lambda x: -x[1])


# ------ DAY 2 -------
baby_names = sc.textFile('data/baby_names.csv')
rows = baby_names.map(lambda line: line.split(','))

# Format file to emulate one in video
# new_file_rows = (rows.map(lambda (year, sex, ethnicity, name, count, rank): ','.join([year, name, ethnicity, sex, count + '\n']))
#  .collect())
# fh = open('data/baby_names.csv', 'w')
# fh.writelines(new_file_rows)
# fh.close()

rows.filter(lambda line: 'MICHAEL' in line).collect()

