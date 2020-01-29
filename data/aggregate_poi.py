import csv

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types
import geopy.distance

APP_NAME = 'POI_AGGREGATION'

class DataSource:
    def __init__(self):
        self.poi_list = None
        self.request_df = None

        conf = SparkConf().setAppName(APP_NAME)
        conf = conf.setMaster('local[*]')
        self.sc = SparkContext(conf=conf)
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
        self.request_df = self.spark.read.csv('/tmp/data/DataSample.csv', inferSchema='true', header='true')
            # count 22025
        print('Request data loaded.  Number of records:', self.request_df.count())
        print(self.request_df.dtypes)

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
            min_distance = []
            for poi in poi_list:
                dist = geopy.distance.vincenty(req_coord, poi['Coord']).km
                if not min_distance or dist < min_distance[1]:
                    min_distance = (poi['POIID'], dist)
            return min_distance[0]

        # Wrapper function returning a udf - User Defined Function
        # used to pass the POI list
        def udf_label(poi_list):
            return functions.udf(lambda vals: calc_nearest_poi(vals, poi_list))

        # Add new column to request data with the name of the nearest POI
        self.request_df = self.request_df.withColumn('NearestPOI', udf_label(self.poi_list)(functions.array('Latitude','Longitude')))
        self.request_df.groupBy('NearestPOI').count().show()

    def calc_poi_distance(self):
        def calc_distance(req_vals, poi_list):
            nearest_poi = req_vals[0]
            req_coord = req_vals[1:]
            for poi in poi_list:
                if nearest_poi != poi['POIID']:
                    continue
                return geopy.distance.vincenty(req_coord, poi['Coord']).km
            return None

        def udf_label(poi_list):
            return functions.udf(lambda vals: calc_distance(vals, poi_list), types.DoubleType())

        # Add new column to request data with the calculated nearest POI
        self.request_df = self.request_df.withColumn('POIDistance', udf_label(self.poi_list)(functions.array('NearestPOI','Latitude','Longitude')))

    def analyze_data(self):
        gdf = self.request_df.groupBy(self.request_df.NearestPOI)
        gdf.agg(functions.mean('POIDistance').alias('AvgDistance'), functions.stddev('POIDistance').alias('StddevDistance')).show()


def main():
    data_source = DataSource()
    data_source.get_data()
    data_source.cleanup_data()
    data_source.label_data()
    data_source.calc_poi_distance()
    data_source.analyze_data()

    data_source.request_df.show(3)

if __name__ == '__main__':
    main()
