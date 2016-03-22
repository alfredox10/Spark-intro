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


