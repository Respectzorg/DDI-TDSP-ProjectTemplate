import pandas as pd
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
from sqlalchemy.types import NVARCHAR, Float, Integer
import pathlib
import yaml


class Connections(object):
    conn = None
    user = None
    passw = None
    server = None
    port = None

    def __init__(self, username, password, server, port, dialect, driver, db):
        self.user = username
        self.passw = password
        self.server = server
        self.port = port
        self.dialect = dialect
        self.driver = driver
        self.db = db

    def __enter__(self):
        return self

    def connect(self):
        eng = create_engine(
            f"{self.dialect}+{self.driver}://{self.user}:{self.passw}@{self.server}:{self.port}/{self.db}",
            max_overflow=-1
        )
        print(f"Connecting to {db} on {self.server}:{self.port}..")
        conn = eng.connect()
        print(f"Connected")
        return conn

    def closeconn(self, conn):
        if conn != None:
            conn.close()

    def close(self):
        self.closeconn(conn)

    @property
    def run_connection(self):
        if self.conn == None:
            self.conn = self.connect()
        return self.conn


def table_exists(table_name: str, connection: object):
    """
    Parameters
    ------
    table_name (type: string) : name of the table to find
    connection (type: sqlalchemy object) : database to find the table in
    Returns
    ------
    boolean
    """
    return connection.dialect.has_table(connection, table_name)


if __name__ == "__main__":
    print("Error: This module shouldn't be calles directly.")
else:
    # Configure server connection variables
    path_parents = pathlib.Path().absolute().parents
    path = pathlib.Path().absolute()
    for parent in path_parents:
        folder_name = str(parent).split('\\')[-1]
        if folder_name == 'Code':
            path = parent

    try:
        with open(str(path) + '\\configuration.yaml') as file:
            yaml_config = yaml.full_load(file)
            db_server = yaml_config['db_server']
            db_port = yaml_config['db_port']
            db_username = yaml_config['db_username']
            db_password = yaml_config['db_password']
            dialect = yaml_config['dialect']
            driver = yaml_config['driver']
            db = yaml_config['db']
    except Exception as e:
        print('A yaml file called "configuration.yaml" should be in a folder called "Code" which should be within called file\'s path.')
        print(e)
    finally:
        # Get connection engine
        pool = Connections(db_username, db_password,
                           db_server, db_port, dialect, driver, db)
        conn_engine = pool.run_connection
