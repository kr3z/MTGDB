import configparser
import mysql.connector
import traceback
import os
import threading

WORKING_DIR = os.path.dirname(os.path.realpath(__file__))

config = configparser.ConfigParser()
config.read(WORKING_DIR+os.sep+'db.properties')

db_host=config.get("db", "db_host")
db_name=config.get("db", "db_name")
db_user=config.get("db", "user")
db_password=config.get("db", "password")

class DBConnection():
    _open_connections = []
    _id_pool = []
    _owned_connections = {}

    def __init__(self):
        self.conn = mysql.connector.connect(host=db_host,
                                   database=db_name,
                                   user=db_user,
                                   password=db_password)

        self.conn.autocommit = False
        self.conn.sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION'
        self.cursor = self.conn.cursor()

        DBConnection._open_connections.append(self.conn)


    def close(self) -> None:
         if self.conn is not None:
            if self.cursor is not None:
                self.cursor.close()
                self.curosr = None
            DBConnection._open_connections.remove(self.conn)
            if threading.get_ident() in DBConnection._owned_connections:
                del DBConnection._owned_connections[threading.get_ident()]
            self.conn.close()
            self.conn = None

    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()
    #def getCursor(self):
    #    return self.cursor

    def start_transaction(self) -> None:
        if not self.conn.in_transaction:
            self.conn.start_transaction(consistent_snapshot=True, isolation_level='READ COMMITTED', readonly=False)

    def in_transaction(self) -> bool:
        return self.conn.in_transaction
    
    def is_connected(self) -> bool:
        return self.conn.is_connected()

    def execute(self,query: str,binds: list = None) -> list:
        res = None
        try:
            if not self.cursor:
                self.cursor = self.conn.cursor()
            self.cursor.execute(query,binds)
            res = self.cursor.fetchall()
        except Exception as error:
            print(error)
            traceback.print_exc()
            if self.conn:
                self.rollback()
        return res
    
    def executemany(self,query: str,binds: list = None) -> list:
        res = None
        try:
            if not self.cursor:
                self.cursor = self.conn.cursor()
            self.cursor.executemany(query,binds)
            res = self.cursor.fetchall()
        except Exception as error:
            print(error)
            traceback.print_exc()
            if self.conn:
                self.rollback()
        return res

    
    @classmethod
    def getConnection(cls) -> 'DBConnection':
        dbconn = cls._owned_connections.get(threading.get_ident())
        if dbconn is None or not dbconn.is_connected():
            if dbconn:
                dbconn.close()
            dbconn = DBConnection()
            cls._owned_connections[threading.get_ident()] = dbconn

        return dbconn
    
    @classmethod
    def singleQuery(cls,query,binds=None):
        conn = None
        res = None
        try:
            conn = cls.getConnection()
            cursor = conn.getCursor()
            cursor.execute(query,binds)
            res = cursor.fetchall()

        except Exception as error:
            print(error)
            traceback.print_exc()
            if conn is not None:
                conn.rollback()
            raise error
        finally:
            if conn is not None:
                if conn.conn.in_transaction:
                    conn.commit()
                conn.close()

        return res

    @classmethod
    def getNextId(cls) -> int:
        if(len(cls._id_pool) == 0):
            conn = cls.getConnection()
            res = conn.execute("SELECT NEXTVAL(id_seq),increment from id_seq")
            #res = cls.singleQuery("SELECT NEXTVAL(id_seq),increment from id_seq")
            next_val = res[0][0]
            increment = res[0][1]
            cls._id_pool.extend(range(next_val,next_val+increment))
        return cls._id_pool.pop(0)