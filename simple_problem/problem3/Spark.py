from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//friends.json")
wordCount = textFile.map(lambda line: (json.loads(line)[0], 1))\
                    .reduceByKey(lambda a, b: a+b)
wordCount.foreach(print)
