# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import json

class ELKConnector(object):
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
        self.conn = Elasticsearch([{'host':info['host'],'port':info['port']}])
        return

    def getCursor(self):
        return self.conn.cursor()

    def getConnection(self):
        return self.conn

    def getQueryForSearchTables(self):
        return ''

    def getQueryForSearchRows(self, table_name):
        return ''

    def checkSavingTable(self, table):
        return False

    def startBeforeProcess(self, data):
        es = self.getConnection()
        for es_index in data['indices']:
            try:
                es.indices.delete(index=es_index)
            except Exception as ex:
                print(ex)

    def startProcess(self, data):
        self.getConnection().index(index=data['db'],
                                   doc_type=data['table'],
                                   body=data['row'])

