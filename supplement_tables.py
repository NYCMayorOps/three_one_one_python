######
## script to generate supplement tables. Run this when shapefile changes.
######
from operator import index
import os
os.environ['USE_PYGEOS'] = '0'
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sqlalchemy as sal
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
gpd.options.use_pygeos = True
from geopandas import GeoDataFrame
from bcpandas import SqlCreds, to_sql


load_dotenv(f'C:\\Users\\{os.getlogin()}\\secrets\\.env')
CONNECTION_STRING = os.getenv('CONNECTION_STRING_311')
GIS_ROOT = Path(os.getenv('GIS_PATH'))
#CONNECTION_STRING = os.getenv('CONNECTION_STRING_SQL_ALCHEMY')
if CONNECTION_STRING is None:
    raise Exception('no connection string found.')

engine = sal.create_engine(CONNECTION_STRING)

print('reading central business districts.')
cbd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles'/ 'CBD_and_BID'/ 'nyc_cbd_1.shp')
#print(cbd_gdf.info())
'''
 #   Column      Non-Null Count  Dtype
---  ------      --------------  -----
 0   cartodb_id  4 non-null      int64
 1   sdname      4 non-null      object
 2   sdlbl       4 non-null      object
 3   shape_leng  4 non-null      float64
 4   shape_area  4 non-null      float64
 5   keep        4 non-null      object
 6   geometry    4 non-null      geometry
'''

cbd_df = pd.DataFrame({'geoid': cbd_gdf['sdlbl'], 'name': cbd_gdf['sdname']})
cbd_df['geoid'] = cbd_df.geoid.str.strip()

print('reading BID shapefile')
bid_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'CBD_and_BID' /'BusinessImprovementDistrict.shp')
#print(bid_gdf.info())

'''
Data columns (total 8 columns):
 #   Column      Non-Null Count  Dtype
---  ------      --------------  -----
 0   BIDID       76 non-null     int64
 1   BID         76 non-null     object
 2   SHAPE_AREA  76 non-null     float64
 3   SHAPE_LEN   76 non-null     float64
 4   borough     76 non-null     object
 5   created     76 non-null     object
 6   modified    75 non-null     object
 7   geometry    76 non-null     geometry
'''

bid_df = pd.DataFrame({'geoid': bid_gdf['BIDID'], 
    'name': bid_gdf['BID'], 
    'boro': bid_gdf['borough'], 
    'created': bid_gdf['created'], 
    'modified': bid_gdf['modified']
    })
bid_df['geoid'] = bid_df.astype(str).geoid.str.strip()
#print(bid_df.head())

print('reading Community District')
cd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'Community_Districts' / 'geo_export_ce9ce611-3d86-479d-be4a-dca285c433a2.shp')

#print(cd_gdf.info())
'''
 #   Column      Non-Null Count  Dtype
---  ------      --------------  -----
 0   boro_cd     71 non-null     float64
 1   shape_area  71 non-null     float64
 2   shape_leng  71 non-null     float64
 3   geometry    71 non-null     geometry
'''
cd_df : pd.DataFrame = pd.DataFrame({'geoid': cd_gdf['boro_cd']})
cd_df['geoid'] = cd_df['geoid'].astype(int).astype(str)
cd_df['geoid'] = cd_df.geoid.str.strip()

print('reading NYPD precincts')
pd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'NYPD' / 'nypd.shp')
'''
 #   Column      Non-Null Count  Dtype
 # ---  ------      --------------  -----
 # 0   precinct    77 non-null     float64
 # 1   shape_area  77 non-null     float64
 # 2   shape_leng  77 non-null     float64
 # 3   geometry    77 non-null     geometry
 #'''
pd_df = pd.DataFrame({'geoid': pd_gdf['precinct']})
pd_df['geoid'] = pd_df['geoid'].astype(int).astype(str)
pd_df['geoid'] = pd_df.geoid.str.strip()

THREE_ONE_ONE_OPS_SERVER=os.getenv('THREE_ONE_ONE_OPS_SERVER')
THREE_ONE_ONE_OPS_DB=os.getenv('THREE_ONE_ONE_OPS_DB')
THREE_ONE_ONE_OPS_USERNAME=os.getenv('THREE_ONE_ONE_OPS_USERNAME')
THREE_ONE_ONE_OPS_PASSWORD=os.getenv('THREE_ONE_ONE_OPS_PASSWORD')

assert THREE_ONE_ONE_OPS_SERVER is not None
assert THREE_ONE_ONE_OPS_DB is not None
assert THREE_ONE_ONE_OPS_USERNAME is not None
assert THREE_ONE_ONE_OPS_PASSWORD is not None

creds = SqlCreds(server=THREE_ONE_ONE_OPS_SERVER,
    database=THREE_ONE_ONE_OPS_DB,
    username=THREE_ONE_ONE_OPS_USERNAME,
    password=THREE_ONE_ONE_OPS_PASSWORD,
)
time0 = datetime.now()
print(f"upload start at {str(time0)}")
#answer.to_sql('ThreeOneOneGeom', engine, schema=None, if_exists='replace', index=False)
#using bcpandas
creds2 = SqlCreds.from_engine(creds.engine)
#print(creds2.engine)
to_sql(bid_df.astype(str), 'business_improvement_district', creds2, index=False, if_exists='replace')
to_sql(cbd_df.astype(str), 'central_business_district', creds2, index=False, if_exists='replace')
to_sql(cd_df.astype(str), 'community_district', creds2, index=False, if_exists='replace')
to_sql(pd_df.astype(str), 'nypd_precinct', creds2, index=False, if_exists='replace')