# -*- coding:UTF-8 -*-
from pyspark import SparkConf,SparkContext
from Singleton import  *

class SparkContexts(Singleton):
    def __init__(self):
        conf = SparkConf().setMaster("local").setAppName("movie")
        self.__sc = SparkContext(conf=conf)

    def get_sc(self):
        return self.__sc

if __name__ == "__main__":
    sc1 = SparkContexts()
    print id(sc1)