from bs4 import BeautifulSoup
import requests
import re
import time
from datetime import date, datetime
import psycopg2


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


def insert_taf_line(airport, timestamp, wind, vis, clouds_magnitudes, clouds_altitudes):
    sql = """INSERT INTO tafs(identifier, time_stamp, visibility, wind, cloud_magnitudes, cloud_altitudes) 
    VALUES(%s, %s, %s, %s, %s::varchar[], %s::int[]) ON CONFLICT DO NOTHING;"""
    conn = None
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            database="wx_compare_database",
            user="jordanstiles",
            password="Isiag53110",
            port="5433")
        cur = conn.cursor()
        cur.execute(sql, (airport, timestamp, vis, wind, clouds_magnitudes, clouds_altitudes,))
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

            # Pull TAFs
            taf_times_patt_FM = "(FM[0-9]{6})"
            taf_times_patt_BECMG = "(BECMG [0-9]{4}/[0-9]{4})"
            taf_times_patt_TEMPO = "(TEMPO [0-9]{4}/[0-9]{4})"
            taf_time_split = re.split(taf_times_patt_FM + "|" + taf_times_patt_BECMG + "|" + taf_times_patt_TEMPO, metar_and_taf[1])
            print("TAF split:")
            taf_time_split = list(filter(None, taf_time_split))
            print(taf_time_split)
            taf_first_element_split = re.split("([0-9]{4})/([0-9]{4})", taf_time_split[0], maxsplit=1)

            taf_valid_start_day = taf_first_element_split[1][:2]
            taf_valid_start_hour = taf_first_element_split[1][2:]
            taf_valid_end_day = taf_first_element_split[2][:2]
            taf_valid_end_hour = taf_first_element_split[2][2:]

            current_day = int(taf_valid_start_day)
            current_hour = int(taf_valid_start_hour)
            end_day = int(taf_valid_end_day)
            end_hour = int(taf_valid_end_hour)


            index = 0
            tempo_index = 0
            tempo_revert_flag = 0
            tempo_override_flag = 0

            # Flags used for testing
            flags_out = 0

            taf_wind, taf_vis, taf_clouds = extractWeather(taf_time_split, index)
            # DO NOT DELETE COMMENT
            # current_clouds = enumerateClouds(current_clouds)
            taf_clouds = ' '.join(taf_clouds)

            # Some TAFs print their ending hour as 24 instead of 23, so this is necessary:
            if end_hour == 24:
                end_hour = 23

            index += 1
            while (concat(current_day,current_hour)) != (concat(end_day, end_hour)):
                current_day_str, current_hour_str = zerofillDate(current_day, current_hour)

                if index + 1 < len(taf_time_split):
                    # What to do if there is a TEMPO line coming to an end
                    if taf_time_split[tempo_index][:5] == "TEMPO" and \
                            current_day_str + current_hour_str == taf_time_split[tempo_index][11:15]:
                        if flags_out: print("TEMPO Down FLAG")

                        tempo_override_flag = 0
                        tempo_revert_flag = 1
                        tempo_index = 0

                    # What to do if there is a TEMPO line starting
                    # (TEMPO lines also supercede all other applicable wx for their time period)
                    if taf_time_split[index][:5] == "TEMPO" and \
                            current_day_str + current_hour_str == taf_time_split[index][6:10]:
                        if flags_out: print("TEMPO Up FLAG")

                        past_taf_wind, past_taf_vis, past_taf_clouds = taf_wind, taf_vis, taf_clouds

                        tempo_wind, tempo_vis, tempo_clouds = \
                            extractCleanWeather(taf_wind, taf_vis, taf_clouds, taf_time_split, index + 1)

                        # DO NOT DELETE COMMENT
                        # current_clouds = enumerateClouds(current_clouds)
                        tempo_clouds = ' '.join(tempo_clouds)

                        tempo_index = index
                        # As mentioned, TEMPOs supercede all other wx
                        tempo_override_flag = 1
                        index += 2

                    # What to do if there is a BECMG line starting (starts after last time in TAF line)
                    elif taf_time_split[index][:5] == "BECMG" and \
                            current_day_str + current_hour_str == taf_time_split[index][-4:]:
                        if flags_out: print("BECMG FLAG")

                        taf_wind, taf_vis, taf_clouds = \
                            extractCleanWeather(taf_wind, taf_vis, taf_clouds, taf_time_split, index + 1)

                        # DO NOT DELETE COMMENT
                        # current_clouds = enumerateClouds(current_clouds)
                        taf_clouds = ' '.join(taf_clouds)

                        tempo_revert_flag = 0
                        index += 2

                    # What to do if there is a FM line starting (starts on the time listed)
                    elif taf_time_split[index][:2] == "FM" and \
                            current_day_str + current_hour_str + "00" == taf_time_split[index][2:9]:
                        if flags_out: print("FM FLAG")

                        taf_wind, taf_vis, taf_clouds = \
                            extractCleanWeather(taf_wind, taf_vis, taf_clouds, taf_time_split, index + 1)

                        # DO NOT DELETE COMMENT
                        # current_clouds = enumerateClouds(current_clouds)
                        taf_clouds = ' '.join(taf_clouds)

                        tempo_revert_flag = 0
                        index += 2
                if flags_out: print("i = " + str(index))

                # Replace wx if tempo line current
                if tempo_override_flag:
                    taf_wind, taf_vis, taf_clouds = tempo_wind, tempo_vis, tempo_clouds

                # Revert to previous wx when tempo line ends
                if tempo_revert_flag:
                    taf_wind, taf_vis, taf_clouds = past_taf_wind, past_taf_vis, past_taf_clouds

                # Split the clouds apart to create 2 lists, one for magnitude (CLR, SCT, etc.) and one for altitude
                magnitude_list, altitude_list = splitClouds(taf_clouds.split())

                # Enumerate the date and time into a standard format for storage
                taf_time = str(enumerateDatetime(current_day_str + current_hour_str))

                print(taf_time + " | " + taf_wind + " | " + taf_vis + " | " + taf_clouds)

                insert_taf_line(airport=airport, timestamp=taf_time, wind=taf_wind, vis=taf_vis,
                                clouds_magnitudes=magnitude_list, clouds_altitudes=altitude_list)

                current_hour += 1
                if current_hour > 23:
                    current_hour = 0
                    current_day += 1
            i+=1

    print('------------')

    t1 = time.time()
    total = t1 - t0
    print("Time taken:")
    print(total)