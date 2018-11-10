# -*- coding: utf-8 -*-
import pyodbc 
from connectors.connector import Connector
import json

class MSSQLConnector(Connector):
    def __init__(self, connection_info):
        Connector.__init__(self, connection_info)
        return self.connect(connection_info)

    def connect(self, info):
        self.conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + \
                                   info['host'] + ',' + str(info['port']) + \
                                   ';DATABASE=' + \
                                   info['db'] + \
                                   ';UID=' + \
                                   info['user'] + \
                                   ';PWD=' + \
                                   info['password'])
        return

    def getQueryForSearchTables(self):
        return "SELECT name FROM sysobjects WHERE xtype='u'"

    def getQueryForSearchRows(self, table_name):
        return "SELECT * FROM %s" % table_name
    
    def parseValue(self, col, value):
        real_value = "'" + str(value) + "'"
        return col[0], real_value
