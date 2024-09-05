######
## an app to join 311 data to CBG to BIDS
######
from operator import index
import os

import logging
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sqlalchemy as sal
from sqlalchemy import create_engine
import numpy as np
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
#bc pandas is a high level wrapper around BCP 
#for high perfomrmance data transfers between pandas and SQL Server
from bcpandas import SqlCreds, to_sql

BACKFILL = False

def main(engine: sal.engine, year: None ):
    with engine.connect() as conn:
        print('reading from 311')
        ninety_days_ago = datetime.now() - timedelta(days=90)
        thirty_days_ago = datetime.now() - timedelta(days=30)
        year_ago = datetime.now() - timedelta(days=365)

        #sql = f'''SELECT * FROM [311SR].[dbo].[SR] WHERE CLOSED_DATE > '{thirty_days_ago}';'''
        sql = f'''SELECT * FROM [311SR].[dbo].[SR] WHERE CLOSED_DATE > '{ninety_days_ago}';'''
        #uncomment the below line for backfilling. This will cause memory error.
        #sql = f'''SELECT * FROM [311SR].[dbo].[SR];'''
        if year:
            sql = f'''SELECT * FROM [311SR].[dbo].[SR] 
                    WHERE CLOSED_DATE > '{year_ago - timedelta(days=(year + 1) * 365)}' 
                    AND CLOSED_DATE < '{year_ago - timedelta(days=year * 365)}';'''
        print('reading 311')
        three11_90_days = pd.read_sql_query(sql, conn)
        print('reading old bids and cbds')
        sql = "SELECT * FROM [311SR].[dbo].[ThreeOneOneGeom];"
        print('getting old geom join')
        #old df is all records in ThreeOneOneGeom
        old_df = pd.read_sql(sql, conn)
        #print(three11_90_days.info())

    
    '''
    RangeIndex: 851212 entries, 0 to 851211
    Data columns (total 32 columns):
     #   Column                   Non-Null Count   Dtype
    ---  ------                   --------------   -----
     0   SR_NUMBER                851212 non-null  object
     1   CREATED_DATE             851212 non-null  datetime64[ns]
     2   CLOSED_DATE              851212 non-null  datetime64[ns]
     3   AGENCY                   851212 non-null  object
     4   CUSTOMER_CT              851212 non-null  object
     5   CUSTOMER_DESCRIPTOR      838846 non-null  object
     6   LOCATION_TYPE            691849 non-null  object
     7   INCIDENT_ZIP             831410 non-null  object
     8   INCIDENT_ADDRESS         799037 non-null  object
     9   INCIDENT_CITY            795525 non-null  object
     10  STATUS                   851212 non-null  object
     11  DUE_DATE                 1188 non-null    datetime64[ns]
     12  LAST_RESOLUTION_ACTION   851212 non-null  object
     13  RESOLUTION_DESC          643294 non-null  object
     14  LAST_RES_ACTION_DATE     851165 non-null  datetime64[ns]
     15  COMMUNITY_BOARD          851212 non-null  object
     16  BOROUGH                  851212 non-null  object
     17  INTAKE_CHANNEL           769875 non-null  object
     18  LAT                      829486 non-null  float64
     19  LON                      829486 non-null  float64
     20  CUSTOMER_FIRST           756583 non-null  object
     21  CUSTOMER_LAST            810865 non-null  object
     22  CUSTOMER_EMAIL           565556 non-null  object
     23  CALLER_COMMENTS          162870 non-null  object
     24  SUBWAY_LINE              8143 non-null    object
     25  SUBWAY_ENTRANCE          1924 non-null    object
     26  SUBWAY_DIRECTION         3739 non-null    object
     27  SUBWAY_STATION_LOCATION  8143 non-null    object
     28  SANITATION_DISTRICT      851212 non-null  object
     29  SANITATION_SECTION       851212 non-null  object
     30  BIN                      851212 non-null  float64
     31  BBL                      731313 non-null  object
    '''
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
    print('reading BID shapefile')
    bid_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'CBD_and_BID' /'BusinessImprovementDistrict.shp')
    #print(bid_gdf.info())
    logging.info(bid_gdf.info())
    logging.info(bid_gdf['geometry'])
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

    pd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'NYPD' / 'nypd.shp')
    '''
     #   Column      Non-Null Count  Dtype
    ---  ------      --------------  -----
     0   precinct    77 non-null     float64
     1   shape_area  77 non-null     float64
     2   shape_leng  77 non-null     float64
     3   geometry    77 non-null     geometry
    '''
    
    bid_gdf = bid_gdf[['geometry', 'BID', 'BIDID']].to_crs('EPSG:4269')
    cbd_gdf = cbd_gdf[['geometry', 'sdname', 'sdlbl']].to_crs('EPSG:4269')
    cd_gdf = cd_gdf[['geometry', 'boro_cd']].to_crs('EPSG:4269')
    pd_gdf = pd_gdf[['geometry', 'precinct']].to_crs('EPSG:4269') 

    three11_90_days = three11_90_days[['LAT', 'LON', 'SR_NUMBER']]
    print(f"len three11_90_days: {len(three11_90_days)}")
    geometry = [Point(xy) for xy in zip(three11_90_days.LON, three11_90_days.LAT)]
    three11_90_days = three11_90_days.drop(['LON', 'LAT'], axis=1)
    three11_gdf = GeoDataFrame(three11_90_days, crs='EPSG:4269', geometry=geometry)
    print(f"len three11_gdf: {len(three11_gdf)}")

    def form_table(three11_gdf: gpd.GeoDataFrame, geometry_gdf: gpd.GeoDataFrame, geom_type: str, geoid_field_name: str, geoid_type=str) -> pd.DataFrame:
        gdf = three11_gdf.sjoin(geometry_gdf, how='inner', predicate='intersects')
        gdf[geoid_field_name] = gdf[geoid_field_name].astype(geoid_type)
        gdf[geoid_field_name] = gdf[geoid_field_name].astype(str)
        df = pd.DataFrame(columns=['sr_number', 'type', 'geoid'])
        # geoid may come in as an int or a string, but it must leave as a string.
        convert_dict = {'sr_number': str, 'type': str, 'geoid': str}
        df = df.astype(convert_dict)
        df['sr_number']= gdf['SR_NUMBER']
        df['type'] = geom_type
        df['geoid'] = gdf[geoid_field_name] #this is already a string
        return df

    #forms table. three11_gdf is past 90 days.
    bid_df : pd.DataFrame = form_table(three11_gdf, bid_gdf, 'BID', 'BIDID', 'Int64') #the string 'Int64' is short for pd.Int64Dtype()
    cbd_df : pd.DataFrame = form_table(three11_gdf, cbd_gdf, 'CBD', 'sdlbl', str)
    cd_df : pd.DataFrame = form_table(three11_gdf, cd_gdf, 'CD', 'boro_cd', 'Int64')
    pd_df : pd.DataFrame = form_table(three11_gdf, pd_gdf, 'PD', 'precinct', 'Int64')
    len_cd_df = len(cd_df)
    print(f"len cd_df: {len(cd_df)}")

    print('concatenating')
    answer = pd.concat([bid_df, cbd_df, cd_df, pd_df]).sort_values(['sr_number', 'geoid'], axis=0, ascending=True)
    answer = answer.reset_index(drop=True)

    #concat the last 90 days to the historic record. Duplicates to be dropped.
    answer = pd.concat([old_df, answer], ignore_index=True)

    answer.sr_number = answer.sr_number.str.strip()
    answer.type = answer.type.str.strip()
    answer.geoid = answer.geoid.str.strip()
    print(answer.info())
    print(answer.head())
    pre_drop_len = len(answer)
    print(f"pre drop len {len(answer)}")

    #drops duplicates on the concatenated table
    answer = answer.drop_duplicates(subset=['sr_number', 'type', 'geoid'],
                                    keep='last',
                                    inplace=False,
                                    ignore_index=True)
    answer = answer.dropna(axis='index', how='any')
    #print(f"stripped answer: {answer.info()}")
    print(f"len dropped: {len(answer)}")
    post_drop_len = len(answer)
    print('uploading to sql')
    THREE_ONE_ONE_OPS_SERVER=os.getenv('THREE_ONE_ONE_OPS_SERVER')
    THREE_ONE_ONE_OPS_DB=os.getenv('THREE_ONE_ONE_OPS_DB')
    THREE_ONE_ONE_OPS_USERNAME=os.getenv('THREE_ONE_ONE_OPS_USERNAME')
    THREE_ONE_ONE_OPS_PASSWORD=os.getenv('THREE_ONE_ONE_OPS_PASSWORD')
    creds = SqlCreds(server=THREE_ONE_ONE_OPS_SERVER,
        database=THREE_ONE_ONE_OPS_DB,
        username=THREE_ONE_ONE_OPS_USERNAME,
        password=THREE_ONE_ONE_OPS_PASSWORD,
        odbc_kwargs={'TrustServerCertificate': 'yes'}
    )
    time0 = datetime.now()
    print(f"upload start at {str(time0)}")
    #answer.to_sql('ThreeOneOneGeom', engine, schema=None, if_exists='replace', index=False)
    #using bcpandas
    creds2 = SqlCreds.from_engine(creds.engine)
    #print(creds2.engine)
    to_sql(answer, 'ThreeOneOneGeom', creds2, index=False, if_exists='replace')
    time1 = datetime.now()
    print(f'upload geometry complete at {str(time1)}')
    print(f'time elapsed: {str(time1 - time0)}')
    print(f"pre drop len {pre_drop_len}")
    print(f"post_drop_len {post_drop_len}")
    #you need to pull the table from SQL into a shapefile

    with engine.connect() as conn:
        sql = "SELECT * FROM [311SR].[dbo].[agg_1d_ago]"
        three11_grouped = pd.read_sql(sql, conn)
        three11_grouped.to_csv(MAYOR_DASHBOARD_ROOT / 'output' / 'three_one_one' / 'grouped_311_by_community_board_yesterday.csv', index=False)

if __name__ == '__main__':
    os.environ['USE_PYGEOS'] = '0'
    load_dotenv(f'C:\\Users\\{os.getlogin()}\\secrets\\.env')
    CONNECTION_STRING = os.getenv('CONNECTION_STRING_311')
    GIS_ROOT = Path(os.getenv('GIS_PATH'))
    MAYOR_DASHBOARD_ROOT = Path(os.getenv('MAYOR_DASHBOARD_ROOT'))
    #CONNECTION_STRING = os.getenv('CONNECTION_STRING_SQL_ALCHEMY')
    if CONNECTION_STRING is None:
        raise Exception('no connection string found.')
    print(f"using connection string {CONNECTION_STRING}")
    print('connecting to SQL Server')
    engine = sal.create_engine(CONNECTION_STRING)

    if BACKFILL:
        for i in range(4, 25):
            print(f"backfilling {i} years ago")
            main(engine, i)
    else:
        main(engine)