import os
import sys

os.environ['SPARK_HOME'] = "/usr/local/spark-2.1.1-bin-hadoop2.7"
sys.path.append("/usr/local/spark-2.1.1-bin-hadoop2.7/python/")

try:
    from pyspark import SparkContext, SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

sc = SparkContext('local')
words = sc.parallelize(["scala", "java", "hadoop", "spark", "akka"])
print words.count()
