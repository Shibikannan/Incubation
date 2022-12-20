# spark
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import functions as f


conf = SparkConf()

spark=SparkSession.builder.appName("Transformation").getOrCreate()

spark.sparkContext.addPyFile("s3://shibi-lz-07/misc/delta-core_2.12-0.8.0.jar")
from delta import *

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
        rawdata=spark.read.parquet(self.LandingPath)
        return rawdata
    def masking(self,data,columns):
        for column in columns:
            try:
                data=data.withColumn("masked_"+column, sha2(data[column], 256))
            except:
                raise Exception('column not found')
            print('masking is done')
        return data
    def transformation(self,data,precision,*columns):
        for column in columns:
            data=data.withColumn(column, data[column].cast(DecimalType(precision)))
            print('transformation is done')
        return data
    def lookup_dataset(self,data,column1,folder_path):
        lookup_location = folder_path
        pii_cols =  column1
        datasetName = 'lookup'
        df_source = data.withColumn("start_date",f.current_date())
        df_source = df_source.withColumn("end_date",f.lit("null"))

        column_lst=[]
        for i in pii_cols:
            column_lst.append(i)
            column_lst.append("masked_"+i)
        column_lst.append("start_date")
        column_lst.append("end_date")

        df_source=df_source.select(*column_lst)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        insert_dict={}
        for i in column_lst:
            insert_dict[i] = "updates."+i

        insert_dict['start_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['end_date'] = "null"

        print(insert_dict)

        _condition = datasetName+".flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName+"."+i for i in [x for x in column_lst if x.startswith("masked_")]])
        
        print(_condition)

        column = ",".join([datasetName+"."+i for i in [x for x in pii_cols]]) 
        print(column)

        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias(datasetName), pii_cols).where(_condition) 
        print(updatedColumnsToInsert)

        stagedUpdates = (
        updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(datasetName).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."flag_active" : "False",
            "end_date" : f.current_date(),
            "flag_active" : "False"
        }
        ).whenNotMatchedInsert(
        values = insert_dict
        ).execute()

        for i in pii_cols:
            data = data.drop(i).withColumnRenamed("masked_"+i,i)
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
    lookup_data=obj.lookup_dataset(transformeddata,appconfig_df.first()['lookup-dataset']['pii-cols-'+i],appconfig_df.first()['lookup-dataset']['data-location-'+i])
    obj.partitionandstaging(lookup_data,appconfig_df.first()['final-'+i][0][0])
    
