# Imports from external libraries
import psycopg2
import pandas as pd
import sys
# Imports from internal libraries
import configs

QUERRIES = "./querries/"


class PG_Database:

    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password

    def create_database_structure(self):
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        querry = f"{QUERRIES}db_structure.sql"
        try:
            cursor.execute(open(querry, "r").read())
            print(f"Created schema from: {querry}")
        except psycopg2.errors.DuplicateTable:
            print(f"Databse allready exists")
        except psycopg2.DatabaseError:
            print(f"Cann not connect to database: {psycopg2.DatabaseError}")
        finally:
            cursor.close()
            conn.close()

    def restore_schema(self):
        drop_querry = "DROP SCHEMA crawldb CASCADE;"
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(drop_querry)
            print("Dropped schema")
        except psycopg2.errors.InvalidSchemaName:
            print("Schema doesent exist. Creating it....")
        finally:
            cursor.close()
            conn.close()
            self.create_database_structure()

    def execute_querry(self, querry):
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        cursor = conn.cursor()
        cursor.execute(querry)
        l = cursor.fetchall()
        df = pd.read_sql(querry, conn)
        cursor.close()
        conn.close()
        return df

    def insert_page(self, site_id, url, http_status,content_type, acces_time):
        querry = f"""
        INSERT INTO crawldb.page (site_id,url, http_status_code,page_content_type ,accessed_time)
        VALUES ({site_id},'{url}', '{http_status}','{content_type}','{acces_time}')
        RETURNING id; 
        """
        ret_id = -1
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(querry)
            ret_id = cursor.fetchone()[0]
        except:
            print(sys.exc_info())
            print(querry)
        finally:
            cursor.close()
            conn.close()
            return ret_id

    def insert_site(self, domain, robots_txt, sitemap):
        querry = f"""
        INSERT INTO crawldb.site (domain, robots_content, sitemap_content) VALUES ('{domain}', '{robots_txt}', '{sitemap}') RETURNING id;
        """
        ret_id = -1
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(querry)
            ret_id = cursor.fetchone()[0]
        except:
            print(sys.exc_info())
            print(querry)
        finally:
            cursor.close()
            conn.close()
            return ret_id

    def insert_link(self,parent_id,child_id):
        querry = f"""
        INSERT INTO crawldb.link (from_page, to_page) VALUES ({parent_id}, {child_id});
        """
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(querry)
        except:
            print(sys.exc_info())
            print(querry)
        finally:
            cursor.close()
            conn.close()

    def insert_page_data(self,page_id,datatype):
        querry = f"""
        INSERT INTO crawldb.page_data (page_id, data_type_code) VALUES ({page_id}, '{datatype}');
        """
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(querry)
        except:
            print("--------------------------------")
            print("Error in: insert_page_data")
            print(sys.exc_info())
            print(querry)
            print("--------------------------------")
        finally:
            cursor.close()
            conn.close()

    def insert_image(self, page_id, filename,accessed_time):
        querry = f"""
        INSERT INTO crawldb.image (page_id, filename,accessed_time) VALUES ({page_id}, '{filename}','{accessed_time}');
        """
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            cursor.execute(querry)
        except:
            print("--------------------------------")
            print("Error in: insert_image")
            print(sys.exc_info())
            print(querry)
            print("--------------------------------")
        finally:
            cursor.close()
            conn.close()



CrawlDB = PG_Database(configs.HOST,
                      configs.USERNAME,
                      configs.PASSWORD)

if __name__ == "__main__":
    print("Running database tests")

    username = "erik"
    password = "1234"
    host = "localhost"

    db = PG_Database(host, username, password)
    # db.create_database_structure()

    db.restore_schema()
    print(db.execute_querry("""SELECT * FROM information_schema.tables 
                            WHERE table_schema = 'crawldb'"""))
