df = spark.read.csv('/tmp/data/DataSample.csv')
    # count 22025

groupedCoordDf = df.select('*').groupBy(df.Latitude,df.Longitude,df.TimeSt).count().filter('count == 1').drop('count')
    # count 17973

joinedDf = df.join(groupedCoordDf, ['Latitude','Longitude'])
