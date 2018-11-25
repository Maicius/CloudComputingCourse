from pymongo import MongoClient
import time

from pyspark.streaming import StreamingContext
from shutil import move
from pyspark import SparkConf
from pyspark import SparkContext
import re
import os

province = ['北京市', '天津市', '上海市', '重庆市', '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省', '安徽省', '福建省', '江西省',
            '山东省', '河南省', '湖北省', '湖南省', '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省', '台湾省', '内蒙古自治区',
            '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区', '香港特别行政区', '澳门特别行政区']
text = "|".join(province)
province_pattern = re.compile(text)

class SimulationRead(object):
    def __init__(self, file_name):
        os.environ['PYSPARK_PYTHON'] = '/anaconda3/python.app/Contents/MacOS/python'
        conf = SparkConf().setAppName("SparkStreamingRead").setMaster("spark://192.168.68.11:7077")
        self.sc = SparkContext.getOrCreate(conf)
        self.get_variables(file_name)
        self.sc.setCheckpointDir(self.streaming_file)
        self.streamContext = StreamingContext(self.sc, 2)

        self.analysis_data()
        self.monitor_data()

    def get_variables(self, file_name):
        variables = self.sc.textFile(file_name).collect()
        self.db_url = variables[0]
        self.db_name = variables[1]
        self.table_name = variables[2]
        self.streaming_file = variables[3]
        self.result_file = variables[4]

    def monitor_data(self):
        self.streamContext.start()
        self.streamContext.awaitTermination()

    def analysis_data(self):
        data = self.streamContext.textFileStream(self.streaming_file)
        # data_rdds =  data.flatMap(lambda line: line.split("\n"))
        rdd = data.flatMap(lambda line: line.split("\n"))
        rdd = rdd.filter(lambda x: len(x) > 0)
        rdd = rdd.map(lambda x: re.findall(province_pattern, str(x)))
        rdd = rdd.filter(lambda x: len(x) > 0)
        rdd = rdd.map(lambda x: (x[0], 1))
        rdd1 = rdd.reduceByKey(lambda x, y: x + y)
        rdd2 = rdd1.updateStateByKey(update_function)
        rdd1.foreachRDD(self.write_result)
        rdd2.foreachRDD(self.write_result)

    def write_result(self, time, x):
        data = x.collect()
        if not data:
            return
        with open(self.result_file + '/PMG1832011.txt', 'a', encoding='utf-8') as a:
            for province in data:
                if str(province[0]) != '':
                    s_write = str(province[0]) + '_' + str(province[1]) + ';'
                    a.write(s_write)
            a.write('\n')
def update_function(new_values, running_count):
    if running_count is None:
        running_count = 0
    return sum(new_values, running_count)

if __name__ =='__main__':
    sr = SimulationRead(file_name='variables.txt')


