from pyspark.sql.types import DecimalType,StringType
from pyspark.sql import functions as f
import json
from pyspark.sql.functions import col, concat_ws,sha2
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import pyspark.sql.utils
spark = SparkSession.builder.appName('Transformation_Casting').getOrCreate()
spark.sparkContext.addPyFile("s3://dharika-landing-zone-nv/Dependencies/delta-core_2.12-0.8.0.jar")
from delta import *
#Create a class with all required functions
class spark_job: 
    # Reading app configuration file from S3 and return config details 
    def __init__(self):  
        configData = spark.sparkContext.textFile("s3://dharika-landing-zone-nv/Configuration/app_configuration.json").collect()
        data       = ''.join(configData)
        self.config_details = json.loads(data)
        self.datasets=['Actives','Viewership']
    def find(self,i):
            dataset_path=self.config_details['transformation-dataset']['source']['data-location']+i+".parquet"
            return dataset_path
    def scd(self):
        lookup_location = self.config_details['lookup-dataset']['data-location']
        pii_cols = self.config_details['lookup-dataset']['pii-cols']
        return lookup_location,pii_cols
    #Function for Reading the data from source
    def read_data(self,path,format):
        df=spark.read.parquet(path)
        return df
    # Function for writing the data to destination
    def write(self,df, file_format,path):
        print("writing")
        try:
            if file_format=="parquet":
                df.write.format(file_format).save(path)
            
            elif file_format=="csv":
                df.write.format(file_format).save(path)
        except Exception as e:
            return e 
    def read_dataset_from_rawzone(self,path):
        loc=path
        #location = self.config_details['transformation-dataset']['source']['data-location']
        data_format = self.config_details['transformation-dataset']['source']['file-format']
        # Collecting destination and destination format for transfering data to staging zone
        return self.read_data(loc,data_format)
    #Reading viewership data from raw_zone
    #Getting the columns to be casted and masked for actives 
    data_casting = {}
    data_masking = {}
    #Getting the columns to be casted and masked for viewership  
    #Defining the function for casting
    def casting(self,casting_dict,df):
        for item in casting_dict.keys():
            if casting_dict[item].split(",")[0] == "DecimalType":
                df = df.withColumn(item,df[item].cast(DecimalType(scale=int(casting_dict[item].split(",")[1]))))
            elif casting_dict[item] == "StringType":
                df = df.withColumn(item,f.concat_ws(",",f.col(item)))
        return df
    def cols_casting(self):
        spark_job.data_casting = self.config_details['transformation-dataset']['transformation-cols'] 
        spark_job.data_masking = self.config_details['transformation-dataset']['masking-cols']
        return 
    #Defining the function for masking   
    def masking(self,df,col_list):
        for column in col_list:
            if column in df.columns:
                print(df.columns)
                df = df.withColumn("masked_"+column,f.sha2(f.col(column),256))
        return df
    def read_delta_table(self,lookup_location,datasetName):
        targetTable = DeltaTable.forPath(spark,lookup_location+datasetName)
        return targetTable
        
    def lookup_dataset(self,df,name):
        lookup_location = self.config_details['lookup-dataset']['data-location']+"Lookup_data"+name
        pii_cols =  self.config_details['lookup-dataset']['pii-cols']
        datasetName = 'lookup'
        df_source = df.withColumn("begin_date",f.current_date())
        df_source = df_source.withColumn("update_date",f.lit("null"))
        
        pii_cols = [i for i in pii_cols if i in df.columns]
            
        columns_needed = []
        insert_dict = {}
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
        source_columns_used = columns_needed + ['begin_date','update_date']
        print(source_columns_used)

        df_source = df_source.select(*source_columns_used)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location+datasetName)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        for i in columns_needed:
            insert_dict[i] = "updates."+i
            
        insert_dict['begin_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['update_date'] = "null"
        
        print(insert_dict)
        
        _condition = datasetName+".flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName+"."+i for i in [x for x in columns_needed if x.startswith("masked_")]])
        
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
            "update_date" : f.current_date()
          }
        ).whenNotMatchedInsert(
          values = insert_dict
        ).execute()

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df
    def raw_to_staging_zone(self,masked_active,name):
        # write active data to staging zone
        print("ok")
        masked_active.write.partitionBy(self.config_details["transformation-dataset"]["partition-cols"]).mode("overwrite").save(self.config_details["transformation-dataset"]["destination"]["data-location"]+name+"/")
            #active_rawzone = spark_job.read_dataset_from_rawzone(i)
            #active_rawzone=active_rawzone.withColumn('user_id',active_rawzone['user_id'].cast(StringType()))
            #ypecasted_active = job.casting(spark_job.data_casting,active_rawzone)
            #masked_active = job.masking(spark_job.data_masking,typecasted_active)
            #job.raw_to_staging_zone(masked_active)
job  = spark_job()
datasets=['Actives','Viewership']
for i in datasets:
    path=job.find(i)
    if i in path:
        print(path)
        try:
            active_rawzone = job.read_dataset_from_rawzone(path)
        except:
            continue
        active_rawzone.printSchema()
        job.cols_casting()
        typecasted_active = job.casting(spark_job.data_casting,active_rawzone)
        masked_active = job.masking(typecasted_active,spark_job.data_masking)
        #print(masked_active)
        masked_active.printSchema()
        job.raw_to_staging_zone(masked_active,i)
        df = job.lookup_dataset(masked_active,i)
        #job.write(masked_active,"parquet","s3://dharika-staging-zone-nv/Final/")
        job.raw_to_staging_zone(df,i)
        #masked_active.show(7)