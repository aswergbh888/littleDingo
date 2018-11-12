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

    def getColumnType(self, code):
        types = {
            cx_Oracle.STRING: 'string',
            cx_Oracle.FIXED_CHAR: 'fixed_char',
            cx_Oracle.NUMBER: 'number',
            cx_Oracle.DATETIME: 'datetime',
            cx_Oracle.TIMESTAMP: 'timestamp',
            cx_Oracle.CLOB: 'clob',
            cx_Oracle.BLOB: 'blob',
        }
        return types.get(code, None)

    def parseValue(self, col, value):
        col_name = col[0]
        col_type = col[1]
        print(col_type)
        if 'datetime' == self.getColumnType(col_type) or \
           'timestamp' == self.getColumnType(col_type):
            value = value.isoformat()
        return col_name, value
