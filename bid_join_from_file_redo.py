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
from sqlalchemy import create_engine, insert, Table, MetaData, text
from sqlalchemy.orm import sessionmaker
import numpy as np
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
#bc pandas is a high level wrapper around BCP 
#for high perfomrmance data transfers between pandas and SQL Server
from bcpandas import SqlCreds, to_sql
from dateutil import relativedelta
from functools import wraps

def time_it(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = datetime.now()
        result = func(*args, **kwargs)
        print(f"{func.__name__} took {datetime.now() - start}")
        return result
    return wrapper

@time_it
def main(filepath: Path):
    '''
    geocode missing values in 311 data by placing the geoid and type into the table ThreeOneOne Geom.
    '''

    #read list of missing values
    initial_time = datetime.now()
    this_datetime = datetime.now()
    print(f'reading {filepath} {datetime.now()}')
    df = pd.read_csv(filepath)
    print("operation took %s seconds", datetime.now() - this_datetime)
    #make the 311 data geospatially aware with a geometry column
    three11_gdf = geofy_311(df)

    #for each table, geocode missing values
    print(f'reading shapefiles {datetime.now()}')
    bid_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'CBD_and_BID' /'BusinessImprovementDistrict.shp')
    cbd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles'/ 'CBD_and_BID'/ 'nyc_cbd_1.shp')
    cd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'Community_Districts' / 'geo_export_ce9ce611-3d86-479d-be4a-dca285c433a2.shp')
    pd_gdf : gpd.GeoDataFrame = gpd.read_file(GIS_ROOT / 'shapefiles' / 'NYPD' / 'nypd.shp')
    ebc_gdf : gpd.GeoDataFrae = gpd.read_file(GIS_ROOT / 'ThreeOneOne' / 'EveryBlockCounts_r1.zip')

    print(f"converting shapefiles to NAD83 (EPSG:4269) {datetime.now()}")
    bid_gdf = bid_gdf[['geometry', 'BID', 'BIDID']].to_crs('EPSG:4269')
    cbd_gdf = cbd_gdf[['geometry', 'sdname', 'sdlbl']].to_crs('EPSG:4269')
    cd_gdf = cd_gdf[['geometry', 'boro_cd']].to_crs('EPSG:4269')
    pd_gdf = pd_gdf[['geometry', 'precinct']].to_crs('EPSG:4269') 
    ebc_gdf = ebc_gdf[['geometry', 'objectid']].to_crs('EPSG:4269')
    #geospatial step

    print(f"geospatial join {datetime.now()}")
    bid_df : pd.DataFrame = form_table(three11_gdf, bid_gdf, 'BID', 'BIDID', 'Int64') #the string 'Int64' is short for pd.Int64Dtype()
    cbd_df : pd.DataFrame = form_table(three11_gdf, cbd_gdf, 'CBD', 'sdlbl', str)
    cd_df : pd.DataFrame = form_table(three11_gdf, cd_gdf, 'CD', 'boro_cd', 'Int64')
    pd_df : pd.DataFrame = form_table(three11_gdf, pd_gdf, 'PD', 'precinct', 'Int64')
    ebc_df : pd.DataFrame = form_table(three11_gdf, ebc_gdf, 'EBC', 'objectid', 'Int64')

    print(f'concatenating {datetime.now()}')
    results = concatenate_tables(bid_df, cbd_df, cd_df, pd_df, ebc_df)
    results = results.reset_index(drop=True)

    #uploading
    print(f"uploading to SQL Server {datetime.now()}")
    engine = connect_mssql()
    upsert_sql(engine, results)
    engine.dispose()
    print("upload complete")


    #create a dataframe
 
    #upload the dataframe.
@time_it
def upsert_sql(engine: sal.engine, results: pd.DataFrame) -> None:
    #upload to a staging table.
    #first, start a session.
    time0 = datetime.now()
    #define target (staging) table
    metadata = sal.MetaData()
    target_table = Table('ThreeOneOneGeom', metadata, autoload_with=engine)
    #upload to staging table
    staging_table_name = 'ThreeOneOneGeomStaging'
    results.to_sql(staging_table_name, engine, schema=None, if_exists='replace', index=False, chunksize=100_000)
    # Define the upsert SQL query using MERGE
    upsert_query = text("EXEC dbo.push_staging_to_311_geom;")
    # Execute the upsert query
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            conn.execute(upsert_query)      
            transaction.commit()
        except Exception as e:
            transaction.rollback()
            print(e)  

@time_it
def concatenate_tables(bid_df: pd.DataFrame, cbd_df: pd.DataFrame, cd_df: pd.DataFrame, pd_df: pd.DataFrame, ebc_df: pd.DataFrame) -> pd.DataFrame:
    results = pd.concat([bid_df, cbd_df, cd_df, pd_df, ebc_df]).sort_values(['sr_number', 'geoid'], axis=0, ascending=True)
    results = results.reset_index(drop=True)
    return results


@time_it
def connect_mssql():
    #connect to sql server
    CONNECTION_STRING = os.getenv('CONNECTION_STRING_311')
    engine = sal.create_engine(CONNECTION_STRING)
    if CONNECTION_STRING is None:
        raise Exception('no connection string found.')
    print(f"using connection string {CONNECTION_STRING}")
    print('connecting to SQL Server')
    return engine

@time_it
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
@time_it
def geofy_311(missing_311: pd.DataFrame) -> gpd.GeoDataFrame:
    missing_311 = missing_311[['LAT', 'LON', 'SR_NUMBER']]
    print(f"len missing_311: {len(missing_311)}")
    geometry = [Point(xy) for xy in zip(missing_311.LON, missing_311.LAT)]
    missing_311 = missing_311.drop(['LON', 'LAT'], axis=1)
    three11_gdf = GeoDataFrame(missing_311, crs='EPSG:4269', geometry=geometry)
    return three11_gdf




if __name__ == '__main__':
    os.environ['USE_PYGEOS'] = '0'
    load_dotenv(f'C:\\Users\\{os.getlogin()}\\secrets\\.env')
    GIS_ROOT = Path(os.getenv('GIS_PATH'))
    path_to_file = GIS_ROOT / 'ThreeOneOne' / 'missing_geocode_attempt_4.csv'
    print(f"using path {path_to_file}")
    main(path_to_file)
    print('The records in the file have been geocoded successfully and uploaded to MSSQL')