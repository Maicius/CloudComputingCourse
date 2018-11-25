from pyspark import SparkContext
from pyspark import SparkConf
import os


class SparkTopK(object):
    def __init__(self, file_name):
        os.environ['PYSPARK_PYTHON'] = '/anaconda3/python.app/Contents/MacOS/python'
        conf = SparkConf().setAppName("SparkTopK").setMaster("spark://192.168.68.11:7077")
        self.sc = SparkContext.getOrCreate(conf)
        self.K, self.data, self.result_dir = self.get_variables(file_name=file_name)
        self.calculate_top_k()

    def get_variables(self, file_name):
        variables = self.sc.textFile(file_name).collect()
        data = self.sc.textFile(variables[1])
        return int(variables[0]), data, variables[2]

    def calculate_top_k(self):
        data = self.data.map(lambda x: x.split(',')).map(lambda x: (x[0], float(x[1]))).reduceByKey(lambda x, y: x + y)
        # data.coalesce(1, True).saveAsTextFile(self.result_dir)
        top_k = data.top(self.K, key=lambda x: x[1])
        top_key = list(map(lambda x: x[0], top_k))
        with open(self.result_dir + 'pmg1832011.txt', 'w', encoding='utf-8') as w:
            for i in range(len(top_key) - 1):
                w.write(top_key[i] + '\n')
            w.write(top_key[-1])

if __name__ =="__main__":
    top_k = SparkTopK(file_name="variables.txt")




