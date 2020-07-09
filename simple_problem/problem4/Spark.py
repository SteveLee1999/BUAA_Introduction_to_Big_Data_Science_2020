from __future__ import print_function
from pyspark import SparkContext
import json

def count_friend(x):
    my_friend = {}
    asymfriends = []
    for friend in x[1]:
        my_friend[friend] = my_friend.get(friend, 0) + 1
    for friend in x[1]:
        if my_friend[friend] == 1:
            asymfriends += (x[0], friend)
    return asymfriends


sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/spark//friends.json")
wordCount = textFile.map(lambda row: (json.loads(row)[0], json.loads(row)[1]))\
                    .map(lambda x: (x[1], x[0]))\
                    .map(lambda x: (x[0], [x[1]]))\
                    .reduceByKey(lambda a, b: a+b)\
                    .map(count_friend)\
                    .flatMap()
wordCount.foreach(print)
