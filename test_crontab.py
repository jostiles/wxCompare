import random
import string
from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2


def insert_test():
    sql = """INSERT INTO test (id) VALUES(%s);"""
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, (random.choice(string.ascii_letters),))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    """Use "SELECT * FROM table;" to see all data in table"""
    """Use "TRUNCATE table;" to delete everything from a table"""


if __name__ == '__main__':
    insert_test()
    file1 = open("wxCompare_data","a")
    file1.write("Hola \n")
    file1.close()

