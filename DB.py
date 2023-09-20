import configparser
import mysql.connector
import traceback
import os

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

    def __init__(self):
        self.conn = mysql.connector.connect(host=db_host,
                                   database=db_name,
                                   user=db_user,
                                   password=db_password)

        self.conn.autocommit = False
        self.conn.sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION'
        self.cursor = self.conn.cursor()

        DBConnection._open_connections.append(self.conn)


    def close(self):
         if self.conn is not None and self.conn.is_connected():
            if self.cursor is not None:
                self.cursor.close()
            
            DBConnection._open_connections.remove(self.conn)
            self.conn.close()

    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()
    def getCursor(self):
        return self.cursor
    
    @staticmethod
    def singleQuery(query,binds=None):
        conn = None
        res = None
        try:
            conn = DBConnection()
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

    @staticmethod
    def getNextId():
        if(len(DBConnection._id_pool) == 0):
            res = DBConnection.singleQuery("SELECT NEXT VALUE FOR id_seq")
            next_val = res[0][0]
            DBConnection._id_pool.extend(range(next_val,next_val+100))
        return DBConnection._id_pool.pop(0)