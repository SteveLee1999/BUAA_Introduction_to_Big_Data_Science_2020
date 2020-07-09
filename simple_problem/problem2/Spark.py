from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//records.json")
wordCount = textFile.map(lambda line: (line[1], [json.loads(line)]))\
                    .reduceByKey(lambda a, b: a+b)\
                    .flatMap(lambda x: [x[0] + item for item in x[1]])
wordCount.foreach(print)
