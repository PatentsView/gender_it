from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import datetime
from datetime import date
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_adhoc_config(database='gender_attribution'):
    """ Gets database credentials for read/write connection
        :keyword config: credentials for our databases
        :return: sqlalchemy pythonic way to connect to databases, database name
    """
    cstr = get_unique_connection_string(database=database)
    engine = create_engine(cstr)
    return engine

def get_unique_connection_string(database='unique_name'):
    database = f'{database}'
    host = '{}'.format(os.environ['HOST'])
    user = '{}'.format(os.environ['USERNAME'])
    password = '{}'.format(os.environ['PASSWORD'])
    port = '{}'.format(os.environ['PORT'])
    return 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(user, password, host, port, database)