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

sc.defaultParallelism # Get default parallelism value from system

one_to_9 = range( 1, 10 )
parallel = sc.parallelize( one_to_9, 3 )
def f(iterator): yield sum(iterator)
parallel.mapPartitions(f).collect()

parallel = sc.parallelize(one_to_9)
parallel.mapPartitions(f).collect()

one = sc.parallelize(range(1,10))
two = sc.parallelize(range(5,15))

one.union(two).collect()

one.intersection(two).collect()

one.union(two).collect()

one.union(two).distinct().collect()

# Create (key, value) pair for Names to Ethnicity
namesToEthnicity = rows.map(lambda n: (n[1], n[2])).groupByKey()

# View values >> Create dictionary for iteratable
namesToEthnicity.map(lambda x: {x[0]: list(x[1])}).take(3)

numbered_rows = baby_names.filter(lambda line: 'Count' not in line).map(lambda line: line.split(','))
numbered_rows.map(lambda n: (n[1], int(n[4]))).reduceByKey(add).sortByKey().take(5)
numbered_rows.map(lambda n: (n[1], int(n[4]))).reduceByKey(add).sortByKey(False).take(5)

names1 = sc.parallelize(['abe', 'abby', 'apple'])
print names1.reduce(add)

names2 = sc.parallelize(['apple', 'beatty', 'beatrice']).map(lambda a: (a, len(a)))
names2.collect()
names2.flatMap(lambda t: [t[1]]).reduce(add)

names1.count()

teams = sc.parallelize(['twins', 'brewers','cubs','white sox','indians','bad news bears'])
teams.takeSample(True, 3)

hockeyteams = sc.parallelize(['wild', 'blackhawks', 'red wings', 'wild', 'oilers', 'wild', 'whalers'])
hockeyteams.map(lambda k: (k, 1)).countByKey().items()