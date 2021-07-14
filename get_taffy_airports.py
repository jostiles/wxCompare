from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2

t0 = time.time()

try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        database="wx_compare_database",
        user="jordanstiles",
        password="Isiag53110",
        port="5433")
    sql = "TRUNCATE taffy_airports;"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
except (Exception, psycopg2.DatabaseError) as error:
    print(error)


Kcode_url = "https://en.wikipedia.org/wiki/List_of_airports_by_ICAO_code:_K"
def pull_Kcode_airports(url):
    url = url
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content,features="html.parser").get_text()

    # conn = psycopg2.connect(
    #     host="localhost",
    #     database="wx_compare_database",
    #     user="jordanstiles",
    #     password="Isiag53110")

    pattern = "K[A-Z]{3}"
    list_of_airports = re.findall(pattern, soup)
    return list_of_airports


def get_metar_and_taf(airport):
    url = "https://www.aviationweather.gov/metar/data?ids=" + airport + "&format=raw&date=&hours=0&taf=on"
    html_content = requests.get(url).text
    soup = BeautifulSoup(html_content, features="html.parser").get_text()

    metar_and_reg_taf_patt = airport + ".+"
    amd_taf_patt = "TAF AMD " + airport + ".+"
    mil_taf_patt = "TAF " + airport + ".+"
    pattern = "(" + metar_and_reg_taf_patt + "|" + amd_taf_patt + "|" + mil_taf_patt + ")"
    # Makes a list of airports that publish both a METAR and a TAF
    metar_and_taf = re.findall(pattern, soup)
    return metar_and_taf


def insert_taffy_airport(airport):
    sql = """INSERT INTO taffy_airports(identifier) VALUES(%s) ON CONFLICT DO NOTHING;"""
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, (airport,))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    """Use "SELECT * FROM table;" to see all data in table"""
    """Use "TRUNCATE table;" to delete everything from a table"""


def get_list_of_taffy_airports(full_list_of_airports):
    i = 0
    taffy_airport_list = []
    # list_of_airports = ['KABE', 'KABI', 'KABQ', 'KABR', 'KABY', 'KACK', 'KACT', 'KACV', 'KACY', 'KADF', 'l,
    for airport in full_list_of_airports:
        metar_and_taf = get_metar_and_taf(airport)
        if len(metar_and_taf) > 1:
            insert_taffy_airport(airport)
            taffy_airport_list.append(airport)
            i += 1
            print(airport)
    return taffy_airport_list

list_of_airports = pull_Kcode_airports(Kcode_url)
list_of_airports = get_list_of_taffy_airports(list_of_airports)
print(list_of_airports)

print("Get taffy time:")
t1 = time.time()
total = t1 - t0
print(total)