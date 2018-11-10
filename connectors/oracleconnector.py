# -*- coding: utf-8 -*-
import cx_Oracle
from connectors.connector import Connector
import json

class OracleConnector(Connector):
    def __init__(self, connection_info):
        Connector.__init__(self, connection_info)
        return self.connect(connection_info)

    def connect(self, info):
        dsn = cx_Oracle.makedsn(info['host'], info['port'], info['db'])
        self.conn = cx_Oracle.connect(info['user'], info['password'], 
                                      dsn, encoding="UTF-8", nencoding="UTF-8")
        return

    def getQueryForSearchTables(self):
        return "SELECT (owner || '.' || table_name) as tableName FROM all_tables"

    def getQueryForSearchRows(self, table_name):
        return "SELECT * FROM %s" % table_name

    def parseValue(self, col, value):
        real_value = "'" + str(value) + "'"
        return col[0], real_value
