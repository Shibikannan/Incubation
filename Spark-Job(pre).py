# spark

from pyspark.conf import SparkConf
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


conf = SparkConf()

spark=SparkSession.builder.appName("Transformation").getOrCreate()

# Fetching the app config path
# conf.get("spark.path")

# Converting it into the spark dataframe
appconfig_df=spark.read.option("multiline","true").format("JSON").load("s3://shibi-lz-07/configuration/app-config.json")

# Got the individual path respectively
# appconfig_df.show()

class SparkJob:
    def __init__(self,LandingPath,RawPath):
        self.LandingPath=LandingPath
        self.RawPath=RawPath
    def ReadingRaw(self):
        # spark.read.parquet(self.LandingPath).write.mode('overwrite').parquet(self.RawPath)
        rawdata=spark.read.parquet(self.RawPath)
        return rawdata
    def masking(self,data,columns):
        for column in columns:
            data=data.withColumn("masked_"+column, sha2(data[column], 256))
            print('masking is done')
        return data
    def transformation(self,data,precision,*columns):
        for column in columns:
            data=data.withColumn(column, data[column].cast(DecimalType(precision)))
            print('transformation is done')
        return data
    def partitionandstaging(self,data,stazing_zone_path):
        print(stazing_zone_path)
        data.withColumn("year", year(col("date"))).withColumn("day", dayofmonth(col("date"))).write.partitionBy('year','month','day').mode('Overwrite').parquet(stazing_zone_path)
        print('Partion done')

# for i in range(5):
#     print(appconfig_df.first()['final-actives'][i])

# appconfig_df.first()['final-viewership'][4][0]

# appconfig_df.first()['final-actives'][0][0]

# obj=SparkJob(appconfig_df.first()[i][1][0])
# rawdata=obj.readRawData()
# appconfig_df.first()['final-'+i][4][0][0]
# appconfig_df.first()['final-'+i][4][0]
# appconfig_df.first()['final-'+i][1]
# appconfig_df.first()['actives'][0][0]

datasets = ['actives','viewership']

for i in datasets:
    print(i)
    obj=SparkJob(appconfig_df.first()[i][1][0],appconfig_df.first()[i][0][0])
    try:
        obj.ReadingRaw()
    except:
        print('No path existed for '+i)
        continue
    
    rawdata=obj.ReadingRaw()
#     for column in appconfig_df.first()['final-'+i][1]:
    maskeddata=obj.masking(rawdata,appconfig_df.first()['final-'+i][1])
    transformeddata=obj.transformation(maskeddata,int(appconfig_df.first()['final-'+i][4][1]),appconfig_df.first()['final-'+i][4][0][0],appconfig_df.first()['final-'+i][4][0][1])
    obj.partitionandstaging(transformeddata,appconfig_df.first()['final-'+i][0][0])
    