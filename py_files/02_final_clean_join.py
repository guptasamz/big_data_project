# %%
# All required Imports
# !pip install boto3
# !pip install censusdata
# !pip install tabulate
# !pip install geopandas
# !pip install plotly

import geopandas as gpd
import plotly.express as px
import boto3
import os
print(os.environ["JAVA_HOME"])
print(os.environ["SPARK_HOME"])
import findspark
findspark.init()
import pandas as pd

from geopandas import GeoDataFrame
from shapely.geometry import Point

from pyspark.sql import SparkSession

# %%
# Defining the Spark interface
spark = SparkSession.builder.config('spark.executor.cores', '8').config('spark.executor.memory', '6g')\
        .config("spark.sql.session.timeZone", "UTC").config('spark.driver.memory', '6g').master("local[26]")\
        .appName("final-project-app").config('spark.driver.extraJavaOptions', '-Duser.timezone=UTC').config('spark.executor.extraJavaOptions', '-Duser.timezone=UTC')\
        .config("spark.sql.datetime.java8API.enabled", "true").config("spark.sql.execution.arrow.pyspark.enabled", "true")\
        .config("spark.sql.autoBroadcastJoinThreshold", -1)\
        .config("spark.driver.maxResultSize", 0)\
        .config("spark.shuffle.spill", "true")\
        .getOrCreate()

# %%
# Paths to the data
trips_data = '../data/mds-trips-bird.parquet'
socio_economic_fp = '../data/socio_economic/2021_census_tract_davidson.geojson'

# %% [markdown]
# Removing all extra information from trips data

# %%
# Reading the data into a pandas dataframe
trips_df = pd.read_parquet(trips_data)

# %%
#Required columns: 
# start_time, end_time, geometry, actual_cost, trip_distance, trip_id, vehicle_id, start_location(Only the starting location)

# %%
# Getting only the required columns
trips_df = trips_df[['start_time', 'end_time', 'geometry', 'actual_cost', 'trip_distance', 'trip_id', 'vehicle_id']]

# %%
# Getting all the starting locations for all the trips as the first poistion in the linestring
trips_df['start_location'] = trips_df.geometry.apply(lambda x: x['coordinates'][0])

# %%
# Renaming the columns and dropping additional columns
trips_df['route_coordinates'] = trips_df['geometry']
trips_df['geometry'] = trips_df['start_location']
trips_df = trips_df.drop('start_location',axis=1)
# Getting the latitude and longitude data from the starting location (renamed to geometry) 
trips_df['start_longitude'] = trips_df.geometry.apply(lambda x: x[0])
trips_df['start_latitude'] = trips_df.geometry.apply(lambda x: x[1])

# %%
# Creating the shapely point to create a geopandas dataframe
geometry = [Point(xy) for xy in zip(trips_df.start_longitude, trips_df.start_latitude)]
# Dropping the columns that are not required
trips_df = trips_df.drop(['start_longitude', 'start_latitude'], axis=1)
# Converting the pandas dataframe into a geopandas dataframe 
trips_gdf = GeoDataFrame(trips_df, crs="EPSG:4326", geometry=geometry)

# %% [markdown]
# Removing all extra information from socio economic data

# %%
# Reading the data into a geopandas dataframe
socio_economic_df = gpd.read_file(socio_economic_fp)

# %%
# Getting the county names 
new_indices = []
county_names = []
for index in socio_economic_df.NAME.tolist():
        county_name = index.split(',')[1].strip().split(' ')[0].strip()
        county_names.append(county_name)
socio_economic_df['county_name'] = county_name

# %%
# Redefining the index as GEOID
socio_economic_df.index = socio_economic_df.GEOID
# List of columns required
socio_economic_cols = ['median_income_last12months','geometry','county_name']
# Getting only the required columns
socio_economic_df = socio_economic_df.drop(columns = [c for c in socio_economic_df.columns if c not in socio_economic_cols])

# %%
# Writing the final socio economic dataframe to the required location
socio_economic_df.to_parquet('../data/final_socio_economic_data.parquet')

# %%
# Reseting the index to create a join
socio_economic_df = socio_economic_df.reset_index()

# %% [markdown]
# ### Now creating the spatial join

# %%
# Checking if the coordinate reference system is consistent before joining the data 
trips_gdf = trips_gdf.to_crs(socio_economic_df.crs)

# %%
# Creating the spatial join between trip starting position and socio economic data to get the area names 
joined_df = gpd.sjoin(trips_gdf, socio_economic_df, op='within')

# %%
# Analyzing the data to check if everything is correct
joined_df

# %%
# Dropping columns that are not required
joined_df = joined_df.drop(['route_coordinates','index_right'],axis=1)
# Removing duplicate rows
joined_df.drop_duplicates(inplace=True)

# %%
# Creating a temporary data frame with only GEOID and geometry to create a join as geometry column was missing 
temp = socio_economic_df[['GEOID','geometry']]
temp['geometry_polygon'] = temp['geometry']
temp = temp[['GEOID','geometry_polygon']]

# %%
# Joining on GEOID and getting the geometry column from socio economic data
joined_df = joined_df.join(temp.set_index('GEOID'),on='GEOID',how='inner')

# %%
# Storing the data as a parquet file to the required location
joined_df.to_parquet('../data/final_joined_data.parquet')


