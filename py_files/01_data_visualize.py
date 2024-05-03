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

import json
import shapely.geometry
import numpy as np
from pyspark.sql import SparkSession
from shapely.geometry import LineString
import plotly.graph_objects as go

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
availability_data = '../data/mds-availability-bird.parquet' 
trips_data = '../data/mds-trips-bird.parquet'
socio_economic_fp = '../data/socio_economic/2021_census_tract_davidson.geojson'

# %% [markdown]
# ### Visualize Census Data

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
# Creating the choropleth plot and saving it as an html file to be view able on any browser
fig = px.choropleth(socio_economic_df, geojson=socio_economic_df.geometry, locations=socio_economic_df.index, color='median_income_last12months',
                            color_continuous_scale="Viridis",
                            range_color=(0, socio_economic_df.median_income_last12months.max()),
                            scope="usa",
                            labels={'median_income_last12months':'Median Income of area:','count_name':'County Name'})
# Zooming into the area of interest
fig.update_geos(fitbounds="locations", visible=True)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
# Writing the file to the required location
fig.write_html('./plots/SocioEconomicDisplay.html')
# fig.show()

# %% [markdown]
# ### Visualize Availability Data

# %%
# Reading the data into a pandas dataframe
avai_df = pd.read_parquet(availability_data)

# %%
# Getting the latitude and longitude values
lat = []
long = []

for i in avai_df.index:
    for key, value in avai_df.geometry.iloc[i].items():
        # print(value)
        if key == 'coordinates':
            # print(value[0])
            long.append(value[0])
            lat.append(value[1])


# %%
# Saving the latitude and longitude values back into dataframe as columns
avai_df['latitude'] = lat
avai_df['longitude'] = long

# %%
# Creating the scatter_mapbox plot and saving it as an html file to be view able on any browser
# Below is my public mapbox token ID that I created 
px.set_mapbox_access_token("pk.eyJ1IjoiZ3VwdGFzYW16IiwiYSI6ImNsZ3d6Zzh0eTAwbjMzcW8wcnJybmp6cmcifQ.4ZGZIjNSFzk6aYjYUT3P1Q")
# For the line mapbox I have given the latitude and longitude values for the points. Zoom = 12 is used to view the area of interest and not the whole mapbox. Categorizing the points using event_type.
fig = px.scatter_mapbox(avai_df, lat="latitude", lon="longitude",color='event_type',
                   size_max=20,zoom=12)
# Giving a name to the figure 
fig.update_layout(
        title = 'Scooters availability display')
# Writing the file to the required location
fig.write_html('./plots/ScoAvai.html')

# %% [markdown]
# ### Visualize Trips data

# %%
# Reading the data into a pandas dataframe
trips_df = pd.read_parquet(trips_data)

# %%
# Taking only the first 500 trips as there are around 97000 trips in the dataset and all of these won't be viewable in the visualization graph as there will be too many overlaps
trips_df = trips_df[:500]

# %%
# Fixing the column names
trips_df['geometry_old'] = trips_df['geometry']
# Converting dictionary into shapely linestring geometry to be used in plotly
trips_df['geometry'] = trips_df.geometry.apply(lambda x: LineString(x['coordinates']))

# %%
# Getting the latitude and longitude values for each linestring and also getting the corresponding vehicle IDs 
lats = []
lons = []
names = []

for feature, name in zip(trips_df.geometry, trips_df.vehicle_id):
    if isinstance(feature, shapely.geometry.linestring.LineString):
        linestrings = [feature]
    elif isinstance(feature, shapely.geometry.multilinestring.MultiLineString):
        linestrings = feature.geoms
    else:
        continue
    for linestring in linestrings:
        x, y = linestring.xy
        lats = np.append(lats, y)
        lons = np.append(lons, x)
        names = np.append(names, [name]*len(y))
        lats = np.append(lats, None)
        lons = np.append(lons, None)
        names = np.append(names, None)


# %%
# Creating the line_mapbox plot and saving it as an html file to be view able on any browser
# Below is my public mapbox token ID that I created 
px.set_mapbox_access_token("pk.eyJ1IjoiZ3VwdGFzYW16IiwiYSI6ImNsZ3d6Zzh0eTAwbjMzcW8wcnJybmp6cmcifQ.4ZGZIjNSFzk6aYjYUT3P1Q")
# For the line mapbox I have given the latitude and longitude values for the linestring and vehicle ID as an identifier for the linestring. Zoom = 12 is used to view the area of interest and not the whole mapbox.
fig = px.line_mapbox(lat=lats, lon=lons, hover_name=names,color=names,zoom=12)
# Giving a name to the figure 
fig.update_layout(
        title = 'Scooters Trips display')
# Writing the file to the required location
fig.write_html('./plots/TripsDisplay.html')


