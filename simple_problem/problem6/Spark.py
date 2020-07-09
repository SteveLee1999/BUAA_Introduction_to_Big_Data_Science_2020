from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//matrix.json")
wordCount = textFile.map(lambda line: mapper(json.loads(line)))\
                    .reduce(lambda a, b: a+b)\
                    .map(matrix_add)
wordCount.foreach(print)


def mapper(record):
    maxI = 10
    maxJ = 10
    return_list = []

    if record[0] == 'a':
        i = record[1]
        for j in range(maxJ+1):
            return_list += ((i, j), [record])
    elif record[0] == 'b':
        j = record[2]
        for i in range(maxI+1):
            return_list += ((i, j), [record])
    else:
        pass

    return return_list


def matrix_add(key, values):
    values = list(values)
    a_rows = filter(lambda x: x[0] == 'a', values)
    b_rows = filter(lambda x: x[0] == 'b', values)

    result = 0
    for a in a_rows:
        for b in b_rows:
            if (a[2] == b[1]):
                result += a[3] * b[3]

    if (result != 0):
        return ((key[0], key[1], result))
