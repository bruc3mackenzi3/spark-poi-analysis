import csv

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
import geopy.distance

APP_NAME = 'POI_AGGREGATION'

class DataSource:
    def __init__(self):
        self.poi_list = None
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
                row['Coord'] = (row['Latitude'], row['Longitude'])
                self.poi_list.append(row)
        print('POI List:', self.poi_list)

        # Load request data and filter bad requests
        self.request_df = self.spark.read.csv('/tmp/data/DataSample.csv', header='true')
            # count 22025
        print('Request data loaded.  Number of records:', self.request_df.count())

    def cleanup_data(self):
        groupedCoordDf = self.request_df.select('*') \
                .groupBy(self.request_df.Latitude,self.request_df.Longitude,self.request_df.TimeSt) \
                .count() \
                .filter('count == 1') \
                .drop('count')
            # count 17973

        self.request_df = self.request_df.join(groupedCoordDf, ['Latitude','Longitude', 'TimeSt'])
        print('Size of joined data:', self.request_df.count())

    def label_data(self):
        ''' Label request data with the ID of the nearest POI
        '''

        # Given the request's and each POI's coordinates calculate each distance
        # and returns the ID of the nearest POI
        # Note: Uses Vincenty's formula from geopy library
        def calc_nearest_poi(req_coord, poi_list):
            min_distance = None
            for poi in poi_list:
                dist = geopy.distance.vincenty(req_coord, poi['Coord'])
                if not min_distance or dist < min_distance[1]:
                    min_distance = (poi['POIID'], dist)
            return min_distance[0]

        # Wrapper function returning a udf - User Defined Function
        # used to pass the POI list
        def udf_label(poi_list):
            return functions.udf(lambda vals: calc_nearest_poi(vals, poi_list))

        # Add new column to request data with the calculated nearest POI
        self.request_df = self.request_df.withColumn('NearestPOI', udf_label(self.poi_list)(functions.array('Latitude','Longitude')))
        self.request_df.groupBy('NearestPOI').count().show()


def main():
    data_source = DataSource()
    data_source.get_data()
    data_source.cleanup_data()
    data_source.label_data()
    data_source.request_df.show()

if __name__ == '__main__':
    main()
