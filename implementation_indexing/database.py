# Imports from external libraries
import sqlite3
# Imports from internal libraries
import configs


class IndexDB:
    def __init__(self, path):
        self.db_path = path

    def get_connection(self):
        connection = sqlite3.connect(self.db_path)
        return connection

    def table_exists(self, name):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        q = """SELECT name FROM sqlite_master
            WHERE type= 'table' AND
            name = ?
            ORDER BY name;
            """
        cursor.execute(q, [name])
        table = cursor.fetchone()
        connection.close()

        if not table:
            return False
        if name in table:
            return True
        else:
            return False

    def init_database(self):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()

        q_IndexWord = """
        CREATE TABLE IndexWord (
        word TEXT PRIMARY KEY
        );
        """

        q_Posting = """
        CREATE TABLE Posting (
        word TEXT NOT NULL,
        documentName TEXT NOT NULL,
        frequency INTEGER NOT NULL,
        indexes TEXT NOT NULL,
        PRIMARY KEY(word, documentName),
        FOREIGN KEY (word) REFERENCES IndexWord(word)
        );
        """

        if not self.table_exists("IndexWord"):
            cursor.execute(q_IndexWord)
        if not self.table_exists("Posting"):
            cursor.execute(q_Posting)
        connection.commit()
        connection.close()

        return self

    def drop_all_tables(self):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        q_drop_index_word = """DROP TABLE IF EXISTS IndexWord"""
        q_drop_posting = """DROP TABLE IF EXISTS Posting"""

        cursor.execute(q_drop_index_word)
        cursor.execute(q_drop_posting)

        connection.commit()
        connection.close()

        return self

    def reset_databse(self):
        self.drop_all_tables().init_database()
        return self


index_database = IndexDB(configs.DB_PATH)

if __name__ == "__main__":
    index_database.reset_databse()
    connection = sqlite3.connect(index_database.db_path)
    c = connection.cursor()
    c.execute('''
        INSERT INTO IndexWord VALUES 
            ('Spar'),
            ('Mercator'), 
            ('Tuš');
    ''')

    c.execute('''
        INSERT INTO Posting VALUES 
            ('Spar', 'spar.si/info.html', 1, '92'),
            ('Mercator', 'mercator.si/prodaja.html', 3, '4,12,55'), 
            ('Mercator', 'tus.si/index.html', 1, '18'),
            ('Tuš', 'mercator.si/prodaja.html', 1, '42');
    ''')

    connection.commit()
    connection.close()
