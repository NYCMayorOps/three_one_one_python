import os
os.environ['USE_PYGEOS'] = '0'
from dotenv import load_dotenv
from pathlib import Path
import bid_join
import sqlalchemy as sal
import logging


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)
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

    for i in range(2000, 2025):
        for j in range(1, 13):
            logging.info(f"backfilling {i}-{j}")
            bid_join.main(engine, i, j)