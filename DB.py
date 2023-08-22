import configparser
import mysql.connector
import traceback

config = configparser.ConfigParser()
config.read('db.properties')

db_host=config.get("db", "db_host")
db_name=config.get("db", "db_name")
db_user=config.get("db", "user")
db_password=config.get("db", "password")

class DBConnection():
    __open_connections = []
    def __init__(self):
        self.conn = mysql.connector.connect(host=db_host,
                                   database=db_name,
                                   user=db_user,
                                   password=db_password)

        self.conn.autocommit = False
        self.conn.sql_mode = 'TRADITIONAL,NO_ENGINE_SUBSTITUTION'
        self.cursor = self.conn.cursor()

        DBConnection.__open_connections.append(self.conn)


    def close(self):
         if self.conn is not None and self.conn.is_connected():
            if self.cursor is not None:
                self.cursor.close()
            
            DBConnection.__open_connections.remove(self.conn)
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
        finally:
            if conn is not None:
                conn.close()

        return res
