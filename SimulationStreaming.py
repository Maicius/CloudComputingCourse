from pyspark import SparkConf
from pyspark import SparkContext
from pymongo import MongoClient
import time
from shutil import move
import os

class SimulationStreaming(object):
    def __init__(self, file_name):
        os.environ['PYSPARK_PYTHON'] = '/anaconda3/python.app/Contents/MacOS/python'
        conf = SparkConf().setAppName("SparkStreamingRead").setMaster("spark://192.168.68.11:7077")
        self.sc = SparkContext.getOrCreate(conf)
        self.get_variables(file_name)
        self.temp_file = '/tmp'
        self.check_file_exist(self.streaming_file + self.temp_file)
        self.connect_mongo()
        self.write_file()

    def get_variables(self, file_name):
        variables = self.sc.textFile(file_name).collect()
        self.db_url = variables[0]
        self.db_name = variables[1]
        self.table_name = variables[2]
        self.streaming_file = variables[3]
        self.result_file = variables[4]
        self.count = 0

    def connect_mongo(self):
        conn = MongoClient(self.db_url.split(':')[0], int(self.db_url.split(':')[1]))
        self.db = conn.get_database(self.db_name)
        self.db_collection = self.db.get_collection(self.table_name)

    def write_file(self):
        temp = []
        for i in self.db_collection.find():
            self.count += 1
            temp.append(i['head']['text'])
            if self.count % 50 == 0:
                temp_time = int(time.time())
                print(temp_time)
                with open(self.streaming_file + self.temp_file + '/head' + str(temp_time) + '.log', 'a', encoding='utf-8') as a:
                    for data in temp:
                        a.write(data + '\n')
                temp = []
                move(self.streaming_file + self.temp_file + '/head' + str(temp_time) + '.log', self.streaming_file)
                time.sleep(1)
    def check_file_exist(self, dir):
        if os.path.exists(dir) == False:
            os.makedirs(dir)


if __name__ =='__main__':
    ss = SimulationStreaming("variables.txt")