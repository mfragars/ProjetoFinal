import mysql.connector


class DBConnection:
    def __init__(self, db_host, db_name, db_user, db_pass):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass

    def getConn(self):
        self.conn = mysql.connector.connect(host=self.db_host, database=self.db_name, user=self.db_user, password=self.db_pass)

        if self.conn.is_connected:
            print("Connected")
            return self.conn
        else:
            print(ConnectionError())

    def getCursor(self):
        self.cursor = self.conn.cursor(prepared=True)
        return self.cursor

    def getCommit(self):
        return self.conn.commit()

    def getCloseConnection(self):
        return self.conn.close()