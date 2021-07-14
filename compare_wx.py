from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2

# print(counter)
# def getListOfMetars():
#     """Get unupdated METARs to compare"""
#     list_of_metars = []
#     conn = None
#     try:
#         conn = psycopg2.connect(
#             host="127.0.0.1",
#             database="wx_compare_database",
#             user="jordanstiles",
#             password="Isiag53110",
#             port="5433")
#         cur = conn.cursor()
#         sql = "SELECT * FROM metars ORDER BY id DESC LIMIT %s;"
#         cur.execute(sql, (counter,))
#         list_of_metars = cur.fetchall()
#         cur.close()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()
#     return list_of_metars


def getMatchList():
    match_list = []
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        sql = "SELECT * FROM tafs JOIN new_metars USING (identifier, time_stamp);"
        cur.execute(sql)
        match_list = cur.fetchall()
        print("Matches:")
        for match in match_list:
            print(match)
        # print(match_list)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return match_list


def compareWind(metar_wind, taf_wind):
    metar_wind_dir = metar_wind[:3]
    metar_wind_mag = metar_wind[3:]
    taf_wind_dir = taf_wind[:3]
    taf_wind_mag = taf_wind[3:]

    direction_delta = abs(int(metar_wind_dir) - int(taf_wind_dir))
    magnitude_delta = abs(int(metar_wind_mag) - int(taf_wind_mag))
    return direction_delta, magnitude_delta


def compareVis(metar_vis, taf_vis):
    pass
    metar_vis = enumerateVis(metar_vis)
    taf_vis = enumerateVis(taf_vis)

    vis_delta = abs(metar_vis - taf_vis)
    return vis_delta


def enumerateVis(vis):
    if vis[:2] == "10" or vis[:1] == "P":
        vis_result = 9999
    elif vis[-2:] == "SM":
        vis_result = round(int(vis[:-2]) * 1609.34)
    else:
        vis_result = int(vis)
    if vis_result > 9999:
        vis_result = 9999
    return vis_result


def compareCloudMagnitudes(metar_cloud_mag_list, taf_cloud_mag_list):
    if len(metar_cloud_mag_list) == 0:
        metar_cloud_mag_list.append(0)
    else:
        metar_cloud_mag_list = enumerateCloudsMags(metar_cloud_mag_list)
    if len(taf_cloud_mag_list) == 0:
        taf_cloud_mag_list.append(0)
    else:
        taf_cloud_mag_list = enumerateCloudsMags(taf_cloud_mag_list)

    delta_list = []
    for index in range(min(len(metar_cloud_mag_list), len(taf_cloud_mag_list))):
        delta = abs(int(metar_cloud_mag_list[index]) - int(taf_cloud_mag_list[index]))
        delta_list.append(delta)
    avg = sum(delta_list) / len(delta_list)
    return avg


def enumerateCloudsMags(cloud_list):
    new_cloud_list = []
    for cloud in cloud_list:
        if cloud[0:3] == "SKC":
            cloud_coverage = 0
        elif cloud[0:3] == "FEW":
            cloud_coverage = 1
        elif cloud[0:3] == "SCT":
            cloud_coverage = 2
        elif cloud[0:3] == "BKN":
            cloud_coverage = 3
        else:
            cloud_coverage = 4

        new_cloud_list.append(cloud_coverage)
    return new_cloud_list


def compareCloudAltitudes(metar_cloud_alt_list, taf_cloud_alt_list):
    if len(metar_cloud_alt_list) == 0:
        metar_cloud_alt_list.append(0)
    else:
        metar_cloud_alt_list = enumerateCloudAlts(metar_cloud_alt_list)
    if len(taf_cloud_alt_list) == 0:
        taf_cloud_alt_list.append(0)
    else:
        taf_cloud_alt_list = enumerateCloudAlts(taf_cloud_alt_list)
    delta_list = []
    min_delta_len = min(len(metar_cloud_alt_list), len(taf_cloud_alt_list))
    if min_delta_len == 0:
        return 999
    else:
        for index in range(min_delta_len):
            delta = abs(metar_cloud_alt_list[index] - taf_cloud_alt_list[index])
            delta_list.append(delta)
        avg = sum(delta_list) / len(delta_list)
        return avg


def enumerateCloudAlts(cloud_list):
    new_cloud_list = []
    for index, cloud in enumerate(cloud_list):
        if cloud != 999:
            cloud = cloud * 100
            new_cloud_list.append(cloud)
    return new_cloud_list


def insert_delta(airport, timestamp, wind_dir, wind_mag, vis, clouds_mag, clouds_alt):
    if clouds_alt == 999:
        sql = """INSERT INTO deltas(identifier, time_stamp, wind_dir, wind_mag, vis, cloud_mag) 
            VALUES(%s, %s, %s::numeric, %s::numeric, %s::numeric, %s::numeric) ON CONFLICT DO NOTHING;"""
        conn = None
        try:
            conn = psycopg2.connect(
                host="127.0.0.1",
                database="wx_compare_database",
                user="jordanstiles",
                password="Isiag53110",
                port="5433")
            cur = conn.cursor()
            cur.execute(sql, (airport, timestamp, wind_dir, wind_mag, vis, clouds_mag,))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        """Use "SELECT * FROM table;" to see all data in table"""
        """Use "TRUNCATE table;" to delete everything from a table"""
    else:
        sql = """INSERT INTO deltas(identifier, time_stamp, wind_dir, wind_mag, vis, cloud_mag, cloud_alt) 
        VALUES(%s, %s, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric) ON CONFLICT DO NOTHING;"""
        conn = None
        try:
            conn = psycopg2.connect(
                host="127.0.0.1",
                database="wx_compare_database",
                user="jordanstiles",
                password="Isiag53110",
                port="5433")
            cur = conn.cursor()
            cur.execute(sql, (airport, timestamp, wind_dir, wind_mag, vis, clouds_mag, clouds_alt,))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
        """Use "SELECT * FROM table;" to see all data in table"""
        """Use "TRUNCATE table;" to delete everything from a table"""


compare_list = getMatchList()
for compare_line in compare_list:
    compare_line_identifier = compare_line[0]
    print(compare_line_identifier)
    compare_line_time = compare_line[1]

    taf_vis = compare_line[2]
    taf_wind = compare_line[3]
    taf_cloud_mags = compare_line[4]
    taf_cloud_alts = compare_line[5]

    metar_vis = compare_line[7]
    metar_wind = compare_line[10]
    metar_cloud_mags = compare_line[9]
    metar_cloud_alts = compare_line[8]

    wind_dir_delta, wind_mag_delta = compareWind(metar_wind, taf_wind)
    # print("wind direction complete")
    # print(wind_dir_delta)
    # print("wind magnitude complete")
    # print(wind_mag_delta)
    vis_delta = compareVis(metar_vis, taf_vis)
    # print("vis complete")
    # print(vis_delta)
    cloud_mag_delta = compareCloudMagnitudes(metar_cloud_mags, taf_cloud_mags)
    # print("cloud magnitude complete")
    # print(cloud_mag_delta)
    cloud_alt_delta = compareCloudAltitudes(metar_cloud_alts, taf_cloud_alts)
    # print("cloud altitude complete")
    # print(cloud_alt_delta)
    insert_delta(airport=compare_line_identifier, timestamp=compare_line_time, wind_dir=wind_dir_delta,
                 wind_mag=wind_mag_delta, vis=vis_delta, clouds_mag=cloud_mag_delta, clouds_alt=cloud_alt_delta)