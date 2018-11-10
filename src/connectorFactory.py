# -*- coding: utf-8 -*-
import pymysql
import pyodbc 
import cx_Oracle
from elasticsearch import Elasticsearch
import json
from mysqlconnector import MySQLConnector
from oracleconnector import OracleConnector
from mssqlconnector import MSSQLConnector
from elkconnector import ELKConnector
from csvconnector import CSVConnector

class ConnectorFactory(object):

    @staticmethod
    def getConnector(connection_info):
        print(connection_info)
        connector_type = connection_info['type'] if 'type' in connection_info else ''

        if connector_type == 'mysql':
            return MySQLConnector(connection_info)

        if connector_type == 'mssql':
            return MSSQLConnector(connection_info)

        if connector_type == 'oracle':
            return OracleConnector(connection_info)

        if  connector_type == 'elasticsearch':
            return ELKConnector(connection_info)

        if connector_type == 'csv':
            return CSVConnector(connection_info)
