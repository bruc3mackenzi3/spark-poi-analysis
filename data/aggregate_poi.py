import csv
import os

import geopy.distance
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types

APP_NAME = 'POI_AGGREGATION'
DEBUG = True


class DataSource:
    def __init__(self):
        ''' Constructor initializing Spark environment
        '''

        self.poi_list = None
        self.req_df = None

        conf = SparkConf().setAppName(APP_NAME)
        conf = conf.setMaster('local[*]')
        self.sc = SparkContext(conf=conf)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def get_data(self):
        ''' Load request and POI data from CSVs.
            self.req_df stores request data as a Spark DataFrame
            self.poi_list contains the POI data as a list
        '''

        # Load POI List
        self.poi_list = []
        with open('/tmp/data/POIList.csv') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['Coord'] = (row['Latitude'], row['Longitude'])
                self.poi_list.append(row)

        # Load request data and filter bad requests
        self.req_df = self.spark.read.csv('/tmp/data/DataSample.csv',
                                          inferSchema='true', header='true')
        if DEBUG:
            print('Request data loaded.  Number of records:', self.req_df.count())
            print(self.req_df.dtypes)
            print('\nPOI data loaded:', self.poi_list)

    def cleanup_data(self):
        ''' Clean data by removing suspected malicious requests.  Such requests
            contain identical Latitude, Longitude and TimeSt values.
            Precondition: Data has been loaded with self.get_data
        '''

        raw_count = self.req_df.count()
        # Create new DataFrame only containing filtered requests
        groupedCoordDf = self.req_df.select('*') \
                .groupBy(
                    self.req_df.Latitude,
                    self.req_df.Longitude,
                    self.req_df.TimeSt) \
                .count() \
                .filter('count == 1') \
                .drop('count')

        # Perform an inner join so flagged records are omitted
        self.req_df = self.req_df.join(
                groupedCoordDf,
                ['Latitude','Longitude', 'TimeSt']
        )
        filtered_count = self.req_df.count()
        print('\n1. Cleanup\n==========\nSize of raw data: {}\nSize of joined data: {}, number of records removed: {}'.format(
                raw_count, filtered_count, raw_count-filtered_count))

    def label_data(self):
        ''' Label each record in request data with the nearest POI
        '''

        # Given a request and all POI coordinates calculate each distance
        # and returns the nearest POI's ID
        # Note: Uses geopy library to calculate distance
        def calc_nearest_poi(req_coord, poi_list):
            min_distance = []
            for poi in poi_list:
                dist = geopy.distance.distance(req_coord, poi['Coord']).km
                if not min_distance or dist < min_distance[1]:
                    min_distance = (poi['POIID'], dist)
            return min_distance[0]

        # Wrapper function returning a udf - User Defined Function
        # used to pass the POI list
        def udf_label(poi_list):
            return functions.udf(lambda vals: calc_nearest_poi(vals, poi_list))

        # Add new column to request data with the name of the nearest POI
        self.req_df = self.req_df.withColumn('NearestPOI', udf_label(self.poi_list)(functions.array('Latitude','Longitude')))

        print('\n2. Label\n========\nCalculated nearest POI to each request.  E.g.:')
        self.req_df.show(3)
        if DEBUG:
            self.req_df.groupBy('NearestPOI').count().show()

    def calc_poi_distance(self):
        ''' Given a request and the nearest POI calculate and return the
            distance
        '''

        # Note: Follows the same pattern of wrapping in a UDF as above
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
        self.req_df = self.req_df.withColumn('POIDistance', udf_label(self.poi_list)(functions.array('NearestPOI','Latitude','Longitude')))

    def analyze_data(self):
        ''' Aggregate request data by nearest POI and calculate mean and
            standard deviation
        '''

        # Part 1
        gdf = self.req_df.groupBy(self.req_df.NearestPOI)
        poi_stats = gdf.agg(functions.mean('POIDistance').alias('AvgDistance'), functions.stddev('POIDistance').alias('StddevDistance'))

        print('\n3. Analysis\n===========')
        poi_stats.show()

    def export_data(self):
        '''
        Export requests to CSV
        '''

        foldername = '/tmp/data/request_data_tmp/'
        filename = '/tmp/data/requests.csv'
        print('Exporting data to csv...')
        self.req_df.write.csv(foldername, header=True)

        # Note: Data is exported into a folder and separate files by partition.
        # Here we can assume one partition so move it with wildcard then delete
        # the folder.
        os.system('mv {}part-00000*.csv {}; rm -rf {}'.format(
                foldername, filename, foldername))
        print('Data available at ' + '/tmp/data/requests.csv')


def main():
    data_source = DataSource()
    data_source.get_data()
    data_source.cleanup_data()
    data_source.label_data()
    data_source.calc_poi_distance()
    data_source.analyze_data()
    data_source.export_data()

    if DEBUG:
        data_source.req_df.show(3)


if __name__ == '__main__':
    main()
