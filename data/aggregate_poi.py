import csv

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
import geopy.distance

APP_NAME = 'POI_AGGREGATION'

class DataSource:
    def __init__(self):
        self.poi_df = None
        self.request_df = None

        conf = SparkConf().setAppName(APP_NAME)
        conf = conf.setMaster('local[*]')
        sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def get_data(self):
        # Load POI List
        self.poi_list = []
        with open('/tmp/data/POIList.csv') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.poi_list.append(row)
        print('POI List:', self.poi_list)

        # Load request data and filter bad requests
        self.request_df = self.spark.read.csv('/tmp/data/DataSample.csv', header='true')
            # count 22025
        print('Data loaded.  Number of records:', self.request_df.count())

    def cleanup_data(self):
        groupedCoordDf = self.request_df.select('*') \
                .groupBy(self.request_df.Latitude,self.request_df.Longitude,self.request_df.TimeSt) \
                .count() \
                .filter('count == 1') \
                .drop('count')
            # count 17973

        self.request_df = self.request_df.join(groupedCoordDf, ['Latitude','Longitude', 'TimeSt'])
        print('Size of joined data:', self.request_df.count())


def main():
    data_source = DataSource()
    data_source.get_data()
    data_source.cleanup_data()
    # data_source.label_data()
    data_source.request_df.show()

if __name__ == '__main__':
    main()
