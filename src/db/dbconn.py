"""MSSQL (Cloud SQL for SQL Server) connection via the Cloud SQL Python Connector +
pytds -- ported from rgmc-gcp-api's src/db/dbconn.py for the BigQuery bridge worker
(see src/services/bigquery_bridge.py / src/workers/bigquery_bridge_worker.py).

rgmc-gcp-api's own DbConn supports several internal databases this worker pool never
touches, resolved by name through its own db_mappings dict; this worker only ever
needs the one database already named by MSSQL_DATABASE (sbic_prod), so that lookup
is dropped entirely.
"""
import sqlalchemy
from google.cloud.sql.connector import Connector

import src.config as config


class DbConn:
    def __init__(self, logger=None):
        self.__logger = logger

    def get_mssql_engine(self):
        connector = Connector()

        def getconn():
            return connector.connect(
                config.MSSQL_INSTANCE,
                driver="pytds",
                user=config.MSSQL_USER,
                password=config.MSSQL_PASSWORD,
                db=config.MSSQL_DATABASE,
            )

        return sqlalchemy.create_engine(
            "mssql+pytds://",
            creator=getconn,
            pool_pre_ping=True,
            pool_recycle=300,
        )

    def main(self):
        return self.get_mssql_engine()
