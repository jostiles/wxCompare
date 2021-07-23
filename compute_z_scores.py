from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2
import math
from statistics import mean


def getListOfAirports():
    list_of_airports_and_timestamps = []
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute("SELECT identifier, time_stamp FROM deltas")
        rows = cur.fetchall()

        for row in rows:
            # row = row[0]
            list_of_airports_and_timestamps.append(row)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return list_of_airports_and_timestamps


def getDataPoint(airport):
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT time_stamp, wind_dir, wind_mag, vis, cloud_mag, cloud_alt FROM deltas WHERE identifier = %s;"
        cur.execute(sql, (airport,))
        data_points = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return data_points


def calcZScore(stddev, mean, data_point):
    return (data_point - mean)/stddev


def insert_airport_and_timestamp(airport, timestamp):
    sql = """INSERT INTO z_scores(identifier, time_stamp) VALUES(%s, %s) ON CONFLICT DO NOTHING;"""
    args = (airport, timestamp,)

    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def getSTDDEVofWindDir():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT STDDEV_POP(wind_dir) FROM deltas;"
        cur.execute(sql)
        stddev = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return stddev


def getSTDDEVofWindMag():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT STDDEV_POP(wind_mag) FROM deltas;"
        cur.execute(sql)
        stddev = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return stddev


def getSTDDEVofVis():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT STDDEV_POP(vis) FROM deltas;"
        cur.execute(sql)
        stddev = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return stddev


def getSTDDEVofCloudMag():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT STDDEV_POP(cloud_mag) FROM deltas;"
        cur.execute(sql)
        stddev = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return stddev


def getSTDDEVofCloudAlt():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT STDDEV_POP(cloud_alt) FROM deltas;"
        cur.execute(sql)
        stddev = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return stddev


def getMeanofWindDir():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT AVG(wind_dir) FROM deltas;"
        cur.execute(sql)
        mean = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return mean


def getMeanofWindMag():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT AVG(wind_mag) FROM deltas;"
        cur.execute(sql)
        mean = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return mean


def getMeanofVis():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT AVG(vis) FROM deltas;"
        cur.execute(sql)
        mean = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return mean


def getMeanofCloudMag():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT AVG(cloud_mag) FROM deltas;"
        cur.execute(sql)
        mean = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return mean


def getMeanofCloudAlt():
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT AVG(cloud_alt) FROM deltas;"
        cur.execute(sql)
        mean = cur.fetchall()[0][0]
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return mean


def insertWindDirZScore(airport, time_stamp, z_score):
    sql = """UPDATE z_scores SET wind_dir = %s::numeric 
        WHERE identifier = %s AND time_stamp = %s;"""
    args = (z_score, airport, time_stamp,)
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertWindMagZScore(airport, time_stamp, z_score):
    sql = """UPDATE z_scores SET wind_mag = %s::numeric 
        WHERE identifier = %s AND time_stamp = %s;"""
    args = (z_score, airport, time_stamp,)
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertVisZScore(airport, time_stamp, z_score):
    sql = """UPDATE z_scores SET vis = %s::numeric 
        WHERE identifier = %s AND time_stamp = %s;"""
    args = (z_score, airport, time_stamp,)
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertCloudMagZScore(airport, time_stamp, z_score):
    sql = """UPDATE z_scores SET cloud_mag = %s::numeric 
        WHERE identifier = %s AND time_stamp = %s;"""
    args = (z_score, airport, time_stamp,)
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insertCloudAltZScore(airport, time_stamp, z_score):
    sql = """UPDATE z_scores SET cloud_alt = %s::numeric 
        WHERE identifier = %s AND time_stamp = %s;"""
    args = (z_score, airport, time_stamp,)
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    t0 = time.time()
    list_of_airports_timestamps = getListOfAirports()
    for airport in list_of_airports_timestamps:
        zScore = 0
        airport = airport[0]
        print(airport)
        wind_dir_stddev = getSTDDEVofWindDir()
        wind_mag_stddev = getSTDDEVofWindMag()
        vis_stddev = getSTDDEVofVis()
        cloud_mag_stddev = getSTDDEVofCloudMag()
        cloud_alt_stddev = getSTDDEVofCloudAlt()

        wind_dir_mean = getMeanofWindDir()
        wind_mag_mean = getMeanofWindMag()
        vis_mean = getMeanofVis()
        cloud_mag_mean = getMeanofCloudMag()
        cloud_alt_mean = getMeanofCloudAlt()

        data_points = getDataPoint(str(airport))
        # print(data_points)
        # if len(data_points) != 0:
        for point in data_points:
            time_stamp = point[0]
            # print(time_stamp)
            wind_dir_value = point[1]
            wind_mag_value = point[2]
            vis_value = point[3]
            cloud_mag_value = point[4]
            cloud_alt_value = point[5]
            # print(value)
            insert_airport_and_timestamp(airport, time_stamp)
            if wind_dir_value != None:
                zScore = round(calcZScore(wind_dir_stddev, wind_dir_mean, wind_dir_value), 4)
                insertWindDirZScore(airport, time_stamp, zScore)
            if wind_mag_value != None:
                zScore = round(calcZScore(wind_mag_stddev, wind_mag_mean, wind_mag_value), 4)
                insertWindMagZScore(airport, time_stamp, zScore)
            if vis_value != None:
                zScore = round(calcZScore(vis_stddev, vis_mean, vis_value), 4)
                insertVisZScore(airport, time_stamp, zScore)
            if cloud_mag_value != None:
                zScore = round(calcZScore(cloud_mag_stddev, cloud_mag_mean, cloud_mag_value), 4)
                insertCloudMagZScore(airport, time_stamp, zScore)
            if cloud_alt_value != None:
                zScore = round(calcZScore(cloud_alt_stddev, cloud_alt_mean, cloud_alt_value), 4)
                insertCloudAltZScore(airport, time_stamp, zScore)


    print('------------')
    t1 = time.time()
    total = t1 - t0
    print("Time taken:")
    print(total)