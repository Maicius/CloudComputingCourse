from pyspark import SparkConf
from pyspark import SparkContext

class SimulationStreaming(object):
    def __init__(self):
        conf = SparkConf().setAppName("SparkTopK").setMaster("spark://127.0.0.1:7077")
        self.sc = SparkContext.getOrCreate(conf)
        pass

    def get_variables(self, file_name):
        variables = self.sc.textFile(file_name).collect()
        data = self.sc.textFile(variables[1])
        return int(variables[0]), data, variables[2]