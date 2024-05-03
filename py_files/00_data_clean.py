# %%
# All required Imports
# !pip install fastparquet

import pandas as pd#
import os
print(os.environ["JAVA_HOME"])
print(os.environ["SPARK_HOME"])
import findspark
findspark.init('C:/Users/Admin/Spark')

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import SparkSession
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# %%
# Defining the Spark interface
spark = SparkSession.builder.config('spark.executor.cores', '2').config('spark.executor.memory', '6g')\
        .config("spark.sql.session.timeZone", "UTC").config('spark.driver.memory', '6g').master("local[26]")\
	  .config("spark.executor.extraJavaOptions","-Xmx2048M -Xms2048M")\
        .appName("final-project-app").config('spark.driver.extraJavaOptions', '-Duser.timezone=UTC').config('spark.executor.extraJavaOptions', '-Duser.timezone=UTC')\
        .config("spark.sql.datetime.java8API.enabled", "true").config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .config("spark.sql.autoBroadcastJoinThreshold", -1)\
        .config("spark.driver.maxResultSize", 0)\
        .config("spark.shuffle.spill", "true")\
	  .config("spark.executor.extraJavaOptions","-XX:+UseG1GC")\
        .getOrCreate()

# %% [markdown]
# ### Fixing Trips data

# %%
# Path to the data
trips_data = '../data/mds-trips-bird-v3.json'
# Reading the data into a spark dataframe
trips_spark_df = spark.read.option("multiline","true").json(trips_data)

# %%
# Reparitioning the dataframe to improve peroformance
trips_spark_df = trips_spark_df.repartition(4)

# %%
# Creating a view for the spark dataframe
trips_spark_df.createOrReplaceTempView("trips_data")

# Listing the columns that are required 
get_columns = [
    'accuracy','actual_cost','device_id','start_time','end_time','geometry','propulsion_type','provider_id','provider_name',
    "standard_cost","timelist","trip_distance","trip_duration","trip_id",'vehicle_id',"vehicle_type"
    ]

# Creating the required SQL query in the format SELECT <col1, col2,..., coln> FROM TABLE_NAME i.e. creating the part within <> 
get_str = ", ".join([c for c in get_columns])

# Getting the required query
query = f"""SELECT {get_str} from trips_data"""
print(query)

# Running the query on the spark dataframe and getting the data 
trips_spark_df = spark.sql(query)

# %%
# Function for getting the value of a key value pair in a dictionary here the key is "$numberLong"
def get_value(x):
    return x['$numberLong']

# Getting the first element from the array of propulsion type 
def fix_propulsion_type(x):
    return x[0]

#Creating spark user defined function to be called by the required columns 
get_value_udf = udf(get_value, StringType())
fix_propulsion_type_udf = udf(fix_propulsion_type,StringType())

# Calling the user defined functions and initializing the values for those columns
trips_spark_df = trips_spark_df.withColumn("start_time", get_value_udf("start_time"))
trips_spark_df = trips_spark_df.withColumn("end_time", get_value_udf("end_time"))
trips_spark_df = trips_spark_df.withColumn("propulsion_type", fix_propulsion_type_udf("propulsion_type"))

# %%
# Just some analysis to check if everything worked as required since this is an action and the previous code were all transformations
#trips_spark_df.show(5,False)

# %%
# Writing the spark dataframe to the required location. Throwing an error message if the code did not work. 
trips_spark_df.write.parquet("../data/mds-trips-bird.parquet")
#try:
#    trips_spark_df.write.parquet("../data/mds-trips-bird.parquet")
#except:
#    print("Some error with writing parquet file. Check whether the file already exists at the location you are writing.")


# %% [markdown]
# ### Fixing Availability data saving it as parquet

# %%
# Path to the data
avai_data = '../data/mds-availability-bird-v3.json'
# Reading the data into a spark dataframe
avai_spark_df = spark.read.option("multiline","true").json(avai_data)

# %%
# Function for getting the value of a key value pair in a dictionary here the key is "$numberLong"
def get_value(x):
    return x['$numberLong']

# Getting the first element from the array of propulsion type 
def fix_propulsion_type(x):
    return x[0]

#Creating spark user defined function to be called by the required columns 
get_value_udf = udf(get_value, StringType())
fix_propulsion_type_udf = udf(fix_propulsion_type,StringType())

# Calling the user defined functions and initializing the values for those columns
avai_spark_df = avai_spark_df.withColumn("event_time", get_value_udf("event_time"))
avai_spark_df = avai_spark_df.withColumn('propulsion_type',fix_propulsion_type_udf('propulsion_type'))

# %%
# Just some analysis to check if everything worked as required since this is an action and the previous code were all transformations
avai_spark_df.show(5,False)

# %%
# Writing the spark dataframe to the required location. Throwing an error message if the code did not work. 
try:
    avai_spark_df.write.parquet("../data/mds-availability-bird.parquet")
except:
    print("Some error with writing parquet file. Check whether the file already exists at the location you are writing.")


# %%
# Stopping Spark
spark.stop()

# %%
# Note: I tried running the above code on a EMR cluster on AWS learner lab. However I was not able to get it to work. With AWS giving me random errors. 
# Please ignore the code below. 

# %% [markdown]
# ### Trying on EMR Cluster - Did not work however I was able to run spark locally on my system and work with Big Data

# %%
# %%file 1_emr.py

# from pyspark import SparkContext, SparkConf
# import json
# from operator import add
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType, ArrayType
# from pyspark.sql import SparkSession

# spark = SparkSession.builder\
#         .appName("final-project-app")\
#         .getOrCreate()

# if __name__ == '__main__':
#   # replace this line with the s3 pass when testing over EMR
#   # conf = SparkConf().setAppName('1_count').set('spark.hadoop.validateOutputSpecs', False)
#   # sc = SparkContext(conf=conf).getOrCreate()

#   try:
#     #Reading the input from s3 bucket
#     trips_data = 's3://a3sambucket/Final_project_data/mds-trips-bird-v3.json'
#     trips_spark_df = spark.read.option("multiline","true").json(trips_data)
    
#     trips_spark_df.createOrReplaceTempView("trips_data")

#     get_columns = [
#         'accuracy','actual_cost','device_id','start_time','end_time','geometry','propulsion_type','provider_id','provider_name',
#         "standard_cost","timelist","trip_distance","trip_duration","trip_id",'vehicle_id',"vehicle_type"
#         ]

#     get_str = ", ".join([c for c in get_columns])

#     query = f"""SELECT {get_str} from trips_data"""
#     print(query)

#     trips_spark_df = spark.sql(query)

#     def get_value(x):
#     return x['$numberLong']

#     def fix_propulsion_type(x):
#         return x[0]

#     get_value_udf = udf(get_value, StringType())
#     fix_propulsion_type_udf = udf(fix_propulsion_type,StringType())

#     trips_spark_df = trips_spark_df.withColumn("start_time", get_value_udf("start_time"))
#     trips_spark_df = trips_spark_df.withColumn("end_time", get_value_udf("end_time"))
#     trips_spark_df = trips_spark_df.withColumn("propulsion_type", fix_propulsion_type_udf("propulsion_type"))

#     trips_spark_df.write.parquet("s3://a3sambucket/mds-trips-bird.parquet")
#     # output.repartition(1).saveAsTextFile("s3://a3sambucket/hw6/outputs/1_count.out")

#   finally:
#     # very important: stop the context. Otherwise you may get an error that context is still alive. if you are on colab just restart the runtime if you face problem
#     #finally is used to make sure the context is stopped even with errors
#     spark.stop()
 
  
#   pass

# %%
# # Please fill your aws credential information here
# credentials = {
# }

# %%
# !pip install boto3
# import boto3, json

# session = boto3.session.Session(**credentials)
# s3 = session.client('s3')

# %%
# # upload script to S3. This assumes that your bucket name is vandy-bigdata. if not then change the  paths here.
# s3.upload_file(Filename='1_emr.py', Bucket='a3sambucket', Key='1_emr.py')

# %%
# # replae with your EMR cluster ID

# def submit_job(app_name, pyfile_uri):
#     emr = session.client('emr')
#     emr.add_job_flow_steps(JobFlowId=CLUSTER_ID, Steps=[{
#         'Name': app_name,
#         'ActionOnFailure': 'CANCEL_AND_WAIT',
#         'HadoopJarStep': {
#             'Args': ['spark-submit',
#                      #'--master', 'yarn',
#                      '--deploy-mode', 'cluster',
#                      pyfile_uri],
#             'Jar': 'command-runner.jar'
#         }}])

# %%
# # submit spark job to emr. Make all the necessary changes to the path
# submit_job(app_name='1_emr', pyfile_uri='s3://a3sambucket/1_emr.py')


