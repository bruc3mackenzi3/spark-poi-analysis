from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

APP_NAME = 'POI_AGGREGATION'

conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster('local[*]')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()


# Load POI List
poi_df = spark.read.csv('/tmp/data/POIList.csv', header='true')
print('POI List:')
poi_df.show()

# Load request data and filter bad requests
df = spark.read.csv('/tmp/data/DataSample.csv', header='true')
    # count 22025
print('Data loaded.  Number of records:', df.count())

groupedCoordDf = df.select('*').groupBy(df.Latitude,df.Longitude,df.TimeSt).count().filter('count == 1').drop('count')
    # count 17973

joinedDf = df.join(groupedCoordDf, ['Latitude','Longitude', 'TimeSt'])
print('Size of joined data:', joinedDf.count())
