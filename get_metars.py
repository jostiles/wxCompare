from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2

t0 = time.time()


def concat(a, b):
    s1 = str(a)
    s2 = str(b)

    # Concatenate both strings
    s = s1 + s2

    # Convert the concatenated string
    # to integer
    c = int(s)

    # return the formed integer
    return c


def zerofill(a):
    a_str = str(a)
    z_a_str = a_str.zfill(2)
    return (z_a_str)


def extractWeather(source, index):
    wind = re.findall("([0-9]{5}G*[0-9]*KT)|(VRB[0-9]{2}G*[0-9]*KT)", source[index])
    if len(wind) == 0:
        wind = ""
    else:
        wind = max(wind[0], key=len)
        if wind[0:3] == "VRB":
            wind = "361" + wind[3:5]
        elif wind[5] == "G":
            wind = wind[0:3] + wind[6:8]
        else:
            wind = wind[0:5]

    vis = re.findall("( [0-9]{4} )|(P*[0-9]+SM)", source[index])
    if len(vis) == 0:
        vis = ""
    else:
        vis = max(vis[0], key=len)
        if len(vis) == 6:
            vis = vis[1:-1]

    current_clouds_list = re.findall("(?:((?:FEW|SCT|BKN|OVC){1}(?:[0-9]{3}))|CLR{1}|SKC{1})",
                                     source[index])
    if len(current_clouds_list) > 0 and current_clouds_list[0] == '':
        current_clouds_list[0] = 'SKC999'
    # print(current_clouds_list)
    clouds = current_clouds_list
    # clouds = ' '.join(current_clouds_list)
    return wind, vis, clouds


def extractCleanWeather(current_wind, current_vis, current_clouds, source, index):
    null_handle_wind = current_wind
    null_handle_vis = current_vis
    null_handle_clouds = current_clouds
    current_wind, current_vis, current_clouds = extractWeather(source, index)
    if current_wind == "": current_wind = null_handle_wind
    if current_vis == "": current_vis = null_handle_vis
    if current_clouds == "": current_clouds = null_handle_clouds
    return current_wind, current_vis, current_clouds


def zerofillDate(current_day, current_hour):
    if len(str(current_day)) == 1:
        current_day_str = zerofill(current_day)
        # print("A")
    else:
        current_day_str = str(current_day)
        # print("B")

    if len(str(current_hour)) == 1:
        current_hour_str = zerofill(current_hour)
        # print("C")
    else:
        current_hour_str = str(current_hour)
        # print("D")
    return current_day_str, current_hour_str


def splitClouds(cloud_list):
    magnitude_list = []
    altitude_list = []
    for cloud in cloud_list:
        magnitude_list.append(cloud[0:3])
        altitude_list.append(cloud[3:6])
    return magnitude_list, altitude_list


def extractTime(source):
    time = re.search("[0-9]{6}Z", source).group()
    return time


def enumerateDatetime(string):
    raw_year = str(datetime.utcnow())[0:4]
    raw_month = str(datetime.utcnow())[5:7]
    raw_date = string[0:2]
    raw_time = string[2:4]
    formatted_date = datetime(year=int(raw_year), month=int(raw_month), day=int(raw_date), hour=int(raw_time))
    return formatted_date


def enumerateVis(vis):
    vis_result = 0
    if vis[:2] == "10" or vis[:2] == "P6":
        vis_result = 9999
    elif vis[-2:] == "SM" and len(vis) == 4:
        print(int(vis[:2]) * 1609.34)
        vis_result = int(vis[:2]) * 1609.34
        # if vis_result > 9999
        print(vis_result)
    elif vis[-2:] == "SM" and len(vis) == 3:
        print(int(vis[:1]) * 1609.34)
        vis_result = int(vis[:1]) * 1609.34
        print(vis_result)
    else:
        print(vis[-2:])
    return vis_result


def getListOfAirports():
    list_of_airports = []
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute("SELECT * FROM taffy_airports")
        # print("The number of parts: ", cur.rowcount)
        rows = cur.fetchall()

        for row in rows:
            row = row[0]
            list_of_airports.append(row)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return list_of_airports


def insert_new_metar(airport, timestamp, wind, vis, clouds_magnitudes, clouds_altitudes):
    sql = """INSERT INTO new_metars(identifier, time_stamp, visibility, cloud_altitudes, cloud_magnitudes, wind) 
    VALUES(%s, %s, %s, %s::int[], %s::varchar[], %s) ON CONFLICT DO NOTHING;"""
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, (airport, timestamp, vis, clouds_altitudes, clouds_magnitudes, wind,))
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
    t0 = time.time()
    i = 0
    j = 0

    counter = 0

    list_of_airports = getListOfAirports()
    # list_of_airports = ['KHRT']

    for airport in list_of_airports:
        print(airport)

        j+=1
        url = "https://www.aviationweather.gov/metar/data?ids=" + airport + "&format=raw&date=&hours=0&taf=on"
        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content, features="html.parser").get_text()

        metar_and_reg_taf_patt = airport + ".+"
        amd_taf_patt = "TAF AMD " + airport + ".+"
        mil_taf_patt = "TAF " + airport + ".+"
        pattern = "(" + metar_and_reg_taf_patt + "|" + amd_taf_patt + "|" + mil_taf_patt + ")"
        # Makes a list of airports that publish both a METAR and a TAF
        metar_and_taf = re.findall(pattern, soup)
        if len(metar_and_taf) > 1:
            print("METAR: " + str(metar_and_taf[0]))
            print("TAF: " + str(metar_and_taf[1]))
            # print(metar_and_taf)
            # Pull METAR info
            metar_time = extractTime(metar_and_taf[0])
            metar_time = str(enumerateDatetime(metar_time))
            print("Metar time: " + str(metar_time))
            metar_wind, metar_vis, metar_clouds_list = extractWeather(metar_and_taf, 0)
            print("Metar Wind: " + str(metar_wind))
            print("Metar Vis: " + str(metar_vis))
            print("Metar clouds:")
            # Split the clouds apart to create 2 lists, one for magnitude (CLR, SCT, etc.) and one for altitude
            magnitude_list, altitude_list = splitClouds(metar_clouds_list)
            print(metar_clouds_list)
            print(magnitude_list)
            print(altitude_list)
            insert_new_metar(airport=airport, timestamp=metar_time, wind=metar_wind,
                                 vis=metar_vis, clouds_magnitudes=magnitude_list, clouds_altitudes=altitude_list)
            counter += 1
    print('------------')

    t1 = time.time()
    total = t1 - t0
    print("Time taken:")
    print(total)