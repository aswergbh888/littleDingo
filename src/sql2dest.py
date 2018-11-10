# -*- coding: utf-8 -*-
import sys
import yaml
from connectorFactory import ConnectorFactory

class SQL2ELK(object):

    def __init__(self, info):
        self.connection = dict()
        for key, value in info.items():
            connector = ConnectorFactory.getConnector(value)
            self.connection[key] = connector 
            
    def deleteIndices(self, elasticsearch):
        es = elasticsearch.getConnection()
        for key, connection in self.connection.items():
            if elasticsearch.getType() == connection.getType():
                continue
            es_index = connection.getDB()
            try:
                es.indices.delete(index=es_index)
            except Exception as ex:
                print(ex)

    def getAllTables(self, connection):
        query = connection.getQueryForSearchTables()
        with connection.getCursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            for row in result:
                table_name = row[0]
                yield table_name

    def getAllRows(self, connection, table_name):
        cols = []
        with connection.getCursor() as cur:
            try:
                query = connection.getQueryForSearchRows(table_name)
                cur.execute(query)
                result = cur.fetchall()
            except Exception as ex:
                print(ex)
                return

            # Prepare the column names
            for desc in cur.description:
                cols.append(desc[0])

        for row in result:
            yield zip(cols, row)
            

    def beforeStart(self, dest):
        data = dict()
        data['indices'] = [connector.getDB() for connector in self.connection.values()
                           if connector.isSource()]
        dest.startBeforeProcess(data)

    def saveToDest(self, dest, db, table, row):
        data = dict()
        data['db'] = db
        data['table'] = table
        data['row'] = row
        dest.startProcess(data)


    def startTransfer(self):
        dest = [connector for connector in self.connection.values()
                if connector.isDestination()]

        source = [connector for connector in self.connection.values()
                  if connector.isSource()]

        for dest_connect in dest:
            self.beforeStart(dest_connect)

        for source_connect in source:
            for table in self.getAllTables(source_connect):

                if not source_connect.checkSavingTable(table):
                    print('===do not save:%s=====' % table)
                    continue

                for row in self.getAllRows(source_connect, table):
                    print('=============%s:%s=================' % (str(source_connect), table))
                    data = dict()

                    for col, value in row:
                        print('col:%s, value:%s' %(col, value))
                        # data[col] = "'" + str(value) + "'"
                        data[col] = value

                    for dest_connect in dest:
                        self.saveToDest(dest_connect, 
                                        source_connect.getDB(), 
                                        table,
                                        data)

if __name__ == '__main__':
    filename = sys.argv[1]
    with open(filename, 'r') as fp:
        read_data = yaml.load(fp)
    
    print('======================')
    sql2elk = SQL2ELK(read_data)
    sql2elk.startTransfer()
