# -*- coding: utf-8 -*-

# TODO: convert to exporter
# see https://github.com/RockyZ/Scrapy-sqlite-item-exporter/blob/master/exporters.py

import sqlite3


class SQLitePipeline(object):
    def __init__(self, database='data.sqlite', table='data'):
        self.database = database
        self.table = table
        self.created_tables = []

    def open_spider(self, spider):
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        table = item.name
        if table not in self.created_tables:
            columns = item.fields.keys()
            self._create_table(table, columns)
            self.created_tables.append(table)
        self._upsert(table, item)
        return item

    def _upsert(self, table, item):
        # TODO
        # https://stackoverflow.com/questions/15277373/sqlite-upsert-update-or-insert
        columns = item.keys()
        values = list(item.values())
        sql = 'INSERT OR REPLACE INTO "%s" (%s) VALUES (%s)' % (
            table,
            ', '.join('`%s`' % x for x in columns),
            ', '.join('?' for x in values),
        )
        self.cursor.execute(sql, values)
        self.connection.commit()

    def _create_table(self, table, columns, keys=None):
        sql = 'CREATE TABLE IF NOT EXISTS "%s" ' % table
        column_define = ['`%s` TEXT' % column for column in columns]
        if keys:
            if len(keys) > 0:
                primary_key = 'PRIMARY KEY (%s)' % ', '.join(keys[0])
                column_define.append(primary_key)
            for key in keys[1:]:
                column_define.append('UNIQUE (%s)' % ', '.join(key))
        sql += '(%s)' % ', '.join(column_define)
        self.cursor.execute(sql)
        self.connection.commit()
