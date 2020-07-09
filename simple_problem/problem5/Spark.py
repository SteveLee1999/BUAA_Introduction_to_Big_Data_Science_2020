from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//dna.json")
wordCount = textFile.map(lambda dnasq: (json.loads(dnasq)[1][:-10], json.loads(dnasq)[0]))
wordCount.foreach(print)
