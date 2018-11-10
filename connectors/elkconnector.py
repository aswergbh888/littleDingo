# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from connectors.connector import Connector
import json

class ELKConnector(Connector):
    def __init__(self, connection_info):
        Connector.__init__(self, connection_info)
        return self.connect(connection_info)

    def connect(self, info):
        self.conn = Elasticsearch([{'host':info['host'],'port':info['port']}])

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

