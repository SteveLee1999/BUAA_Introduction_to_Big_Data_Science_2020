from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//books.json")
wordCount = textFile.flatMap(lambda book: (word, [book]) for word in json.loads(book)[1].split(' '))\
                    .reduceByKey(lambda a, b: a + b)
wordCount.foreach(print)
