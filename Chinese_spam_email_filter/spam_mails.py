from pyspark.sql import Row,functions
from pyspark.ml import Pipeline
from pyspark.ml.linalg import Vector,Vectors
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer,HashingTF, Tokenizer, IDF
from pyspark.ml.classification import LogisticRegression,LogisticRegressionModel,BinaryLogisticRegressionSummary, LogisticRegression
from pyspark.ml.feature import Word2Vec
from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
from pyspark.sql import SparkSession
import csv23
import jieba
import re
import codecs
import numpy as np
import sys
mail_str = r'[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+){0,4}$'
ip_str = r'(([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])\.){3}([01]{0,1}\d{0,1}\d|2[0-4]\d|25[0-5])'


def is_chinese(uchar):
    if uchar >= u'\u4e00' and uchar<=u'\u9fa5':
        return True
    else:
        return False


def is_number(uchar):
    if uchar >= u'\u0030' and uchar<=u'\u0039':
        return True
    else:
        return False


def is_alphabet(uchar):
    if (uchar >= u'\u0041' and uchar<=u'\u005a') or (uchar >= u'\u0061' and uchar<=u'\u007a'):
        return True
    else:
        return False


def is_other(uchar):
    if not (is_chinese(uchar) or is_number(uchar) or is_alphabet(uchar)):
        return True
    else:
        return False


def divide_word(content):
    content = content.strip()
    words = []
    pattern_1 = re.compile(mail_str)
    pattern_2 = re.compile(ip_str)
    for _ in pattern_1.finditer(content):
        words.append(_.group())
    for _ in  pattern_2.finditer(content):
        words.append(_.group())

    for i in range(len(content)):
        if is_chinese(content[i]):
            for word in jieba.cut(content[i:], cut_all = False):
                word = word.strip()
                if len(word) > 0:
                    words.append(word)
            break
    # print words
    return words

spark = SparkSession.builder.master("local").appName("chinese-spam-mails").getOrCreate()


if __name__ == '__main__':
    train_words = []
    train_label = []
    train_rddtable = []
    with csv23.open_reader("train.csv", encoding='utf-8') as csv_reader:
        is_first = True
        for i, row in enumerate(csv_reader):
            if is_first:
                is_first = False
                continue
            if i > 20000:
                break
            words = divide_word(row[0])
            train_words.append(words)
            train_label.append(int(row[1]))
            train_rddtable.append((words, int(row[1])))
    
    test_words = []
    test_rddtable = []
    
    
    with csv23.open_reader("test.csv", encoding='utf-8') as csv_reader:
        for i, row in enumerate(csv_reader):
            if i == 0:
                continue
            words = divide_word(row[0])
            test_words.append(words)
            test_rddtable.append((words,))
    
    
    train_stringCSVRDD = spark.sparkContext.parallelize(train_rddtable)
    train_data = spark.createDataFrame(train_stringCSVRDD, ['words', 'label'])
    
    test_stringCSVRDD = spark.sparkContext.parallelize(test_rddtable)
    test_data = spark.createDataFrame(test_stringCSVRDD, ['words'])

    hashingTF = HashingTF(inputCol = 'words', outputCol='vector', numFeatures= 100)
    # learner = LogisticRegression().setLabelCol("label").setFeaturesCol("vector").setRegParam(0.3).setElasticNetParam(0.8)
    learner = RandomForestClassifier(labelCol= "label", featuresCol= "vector", impurity='gini')
    pipeline = Pipeline(stages= [hashingTF, learner])
    model = pipeline.fit(train_data)
    
    lr_prediction = model.transform(test_data)
    lr_prediction = lr_prediction.select('prediction').collect()
    lr_prediction = [int(p[0]) for p in lr_prediction]
    with open('result.txt', 'w') as f:
        for i in range(len(lr_prediction)):
            f.write(str(lr_prediction[i]) + '\n')
