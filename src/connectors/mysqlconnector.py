# -*- coding: utf-8 -*-
import pymysql
from pymysql.constants import FIELD_TYPE
from connectors.connector import Connector
import json

class MySQLConnector(Connector):
    def __init__(self, connection_info):
        Connector.__init__(self, connection_info)
        return self.connect(connection_info)

    def connect(self, info):
        self.conn = pymysql.connect(host=info['host'],
                                    port=info['port'],
                                    user=info['user'],
                                    passwd=info['password'],
                                    db=info['db'],
                                    charset='utf8')
        return
 
    def getQueryForSearchTables(self):
        return 'SHOW TABLES'

    def getQueryForSearchRows(self, table_name):
        return "SELECT * FROM %s" % table_name

    def getColumnType(self, code):
        types = {
            FIELD_TYPE.DECIMAL: 'decimal',
            FIELD_TYPE.TINY: 'tiny',
            FIELD_TYPE.SHORT: 'short',
            FIELD_TYPE.LONG: 'long',
            FIELD_TYPE.FLOAT: 'float',
            FIELD_TYPE.DOUBLE: 'double',
            FIELD_TYPE.NULL: 'null',
            FIELD_TYPE.TIMESTAMP: 'timestamp',
            FIELD_TYPE.LONGLONG: 'long',
            FIELD_TYPE.INT24: 'int',
            FIELD_TYPE.DATE: 'date',
            FIELD_TYPE.TIME: 'time',
            FIELD_TYPE.DATETIME: 'datetime',
            FIELD_TYPE.YEAR: 'year',
            FIELD_TYPE.NEWDATE: 'date',
            FIELD_TYPE.VARCHAR: 'varchar',
            FIELD_TYPE.BIT: 'bit',
            FIELD_TYPE.JSON: 'json',
            FIELD_TYPE.NEWDECIMAL: 'decimal',
            FIELD_TYPE.ENUM: 'enum',
            FIELD_TYPE.SET: 'set',
            FIELD_TYPE.TINY_BLOB: 'blob',
            FIELD_TYPE.MEDIUM_BLOB: 'blob',
            FIELD_TYPE.LONG_BLOB: 'blob',
            FIELD_TYPE.BLOB: 'blob',
            FIELD_TYPE.VAR_STRING: 'varchar',
            FIELD_TYPE.STRING: 'string',
            FIELD_TYPE.GEOMETRY: 'geometry'
        }
        return types.get(code, None)

    def parseValue(self, col, value):
        col_name = col[0]
        col_type = col[1]
        if 'datetime' == self.getColumnType(col_type) or \
           'timestamp' == self.getColumnType(col_type):
            value = value.isoformat()
        return col_name, value
