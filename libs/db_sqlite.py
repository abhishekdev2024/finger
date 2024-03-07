import sqlite3
from sqlite3 import Error
from libs.db import Database
from termcolor import colored
from itertools import zip_longest
from libs.config import (FINGERPRINTS_TABLENAME, SONGS_TABLENAME)

class SQLDatabase(Database):
    TABLE_SONGS = SONGS_TABLENAME
    TABLE_FINGERPRINTS = FINGERPRINTS_TABLENAME
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.text_factory = str

        self.cur = self.conn.cursor()
        
        self.create_tables()  # Check and create tables if they don't exist

        print(colored('sqlite - connection opened','white',attrs=['dark']))
        
        
        
    def create_tables(self):
        try:
            # Create songs table if not exists
            self.cur.execute('''CREATE TABLE IF NOT EXISTS {} (
                                id INTEGER PRIMARY KEY,
                                name TEXT NOT NULL,
                                filehash TEXT NOT NULL UNIQUE)'''.format(SONGS_TABLENAME))
            
            # Create fingerprints table if not exists
            self.cur.execute('''CREATE TABLE IF NOT EXISTS {} (
                                id INTEGER PRIMARY KEY,
                                song_fk INTEGER NOT NULL,
                                hash TEXT NOT NULL,
                                offset INTEGER NOT NULL,
                                FOREIGN KEY(song_fk) REFERENCES {}(id))'''.format(FINGERPRINTS_TABLENAME,SONGS_TABLENAME))
            print(colored('sqlite - tables created','white',attrs=['dark']))
            self.conn.commit()
        except Error as e:
            print(colored(f"Error creating tables: {e}", 'red'))
                

    def __del__(self):
        self.conn.commit()
        self.conn.close()
        print(colored('sqlite - connection closed','white',attrs=['dark']))


    def query(self,query,values=[]):
        self.cur.execute(query,values)

    def executeOne(self, query, values = []):
        self.cur.execute(query, values)
        return self.cur.fetchone()

    def executeAll(self, query, values = []):
        self.cur.execute(query, values)
        return self.cur.fetchall()

    def buildSelectQuery(self, table, params):
        conditions = []
        values = []

        for k, v in enumerate(params):
            key = v
            value = params[v]
            conditions.append("%s = ?" % key)
            values.append(value)

            conditions = ' AND '.join(conditions)
            query = "SELECT * FROM %s WHERE %s" % (table, conditions)
            return {"query": query,"values": values }
    def findOne(self, table, params):
        select = self.buildSelectQuery(table, params)
        return self.executeOne(select['query'], select['values'])    
    

    def findAll(self, table, params):
        select = self.buildSelectQuery(table, params)
        return self.executeAll(select['query'], select['values'])
    


    def insert(self, table, params):
        keys = ', '.join(params.keys())
        values = tuple(params.values())  # Convert values to a tuple
        
        placeholders = ', '.join(['?' for _ in range(len(params))])
        
        query = "INSERT INTO {} ({}) VALUES ({})".format(table, keys, placeholders)
        self.cur.execute(query, values)
        self.conn.commit()

        return self.cur.lastrowid
    

    def insertMany(self, table, columns, values):
        def grouper(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return (filter(None, values) for values in zip_longest(fillvalue=fillvalue, *args))
        

        for split_values in grouper(values, 1000):
            query = "INSERT OR IGNORE INTO %s (%s) VALUES (?, ?, ?)" % (table, ", ".join(columns))
            self.cur.executemany(query, split_values)
        self.conn.commit()


    def get_song_hashes_count(self, song_id):
        query = 'SELECT count(*) FROM %s WHERE song_fk = %d' % (self.TABLE_FINGERPRINTS, song_id)
        rows = self.executeOne(query)
        return int(rows[0])   
 
    