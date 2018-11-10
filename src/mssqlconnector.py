# -*- coding: utf-8 -*-
import pyodbc 
import json

class MSSQLConnector(object):
    def __init__(self, connection_info):
        print(connection_info)
        self.conn = None
        self.connection_info = connection_info
        self.type = connection_info['type'] if 'type' in connection_info else ''
        self.db = connection_info['db'] if 'db' in connection_info else ''
        self.saving_tables = connection_info['tables'] if 'tables' in connection_info else ''
        self.role = connection_info['role'] if 'role' in connection_info else ''

        return self.connect(connection_info)

    def __str__(self):
        return '%s:%s' % (self.type, self.db)

    def getRole(self):
        return self.role

    def isSource(self):
        return self.role == 'source'

    def isDestination(self):
        return self.role == 'destination'

    def getType(self):
        return self.type

    def getDB(self):
        return self.db.lower()

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

    def getCursor(self):
        return self.conn.cursor()

    def getConnection(self):
        return self.conn

    def getQueryForSearchTables(self):
        return "SELECT name FROM sysobjects WHERE xtype='u'"

    def getQueryForSearchRows(self, table_name):
        return "SELECT * FROM %s" % table_name

    def checkSavingTable(self, table):
        if self.saving_tables == '':
            return True
        table_name = table.split('.')[1] if '.' in table else ''
        return table_name in self.saving_tables
