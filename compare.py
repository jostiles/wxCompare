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
    wind = re.findall("([0-9]{5}G*[0-9]*KT)|(VRB[0-9]{2}KT)", source[index])
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
        current_clouds_list[0] = 'SKC'
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


def enumerateClouds(cloud_list):
    for index, cloud in enumerate(cloud_list):
        cloud_coverage = 69
        if cloud[0:3] == "SKC":
            cloud_coverage = 0
        elif cloud[0:3] == "FEW":
            cloud_coverage = 1
        elif cloud[0:3] == "SCT":
            cloud_coverage = 2
        elif cloud[0:3] == "BKN":
            cloud_coverage = 3
        elif cloud[0:3] == "OVC":
            cloud_coverage = 4
        cloud_altitude = cloud[3:6] + "00"
        cloud_list[index] = str(cloud_coverage) + "@" + cloud_altitude
    return cloud_list


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


url = "https://en.wikipedia.org/wiki/List_of_airports_by_ICAO_code:_K"
html_content = requests.get(url).text
soup = BeautifulSoup(html_content,features="html.parser").get_text()

# conn = psycopg2.connect(
#     host="localhost",
#     database="wx_compare_database",
#     user="jordanstiles",
#     password="Isiag53110")

pattern = "K[A-Z]{3}"
list_of_airports = re.findall(pattern, soup)
print(list_of_airports)

i = 0
j = 0
taffy_airport_list = []
# list_of_airports = ['KABE', 'KABI', 'KABQ', 'KABR', 'KABY', 'KACK', 'KACT', 'KACV', 'KACY', 'KADF', 'KADW', 'KAEG', 'KAEX', 'KAFF', 'KAFW', 'KAGC', 'KAGS', 'KAHN', 'KAIA', 'KALB', 'KALI', 'KFHU', 'KALO', 'KALS', 'KALW', 'KAMA', 'KANB', 'KAND', 'KAOO', 'KAPA', 'KAPC', 'KAPF', 'KAPN', 'KARA', 'KART', 'KASD', 'KASE', 'KAST', 'KATL', 'KATW', 'KATY', 'KAUG', 'KAUS', 'KAUW', 'KAVL', 'KAVP', 'KAXN', 'KAZO', 'KBAB', 'KBAD', 'KBAF', 'KBAM', 'KBBD', 'KBBG', 'KBCB', 'KBCE', 'KBDL', 'KBDN', 'KBDR', 'KBED', 'KBFD', 'KBFF', 'KBFI', 'KBFL', 'KBFM', 'KBGM', 'KBGR', 'KBHB', 'KBHM', 'KBIF', 'KBIH', 'KBIL', 'KBIS', 'KBIX', 'KBJC', 'KBJI', 'KBKE', 'KBKW', 'KBLF', 'KBLH', 'KBLI', 'KBLV', 'KBMG', 'KBMI', 'KBNA', 'KBNO', 'KBOI', 'KBOS', 'KBPI', 'KBPK', 'KBPT', 'KBRD', 'KBRL', 'KBRO', 'KBTL', 'KBTM', 'KBTR', 'KBTV', 'KBUF', 'KBUR', 'KBVI', 'KBVO', 'KBWG', 'KBWI', 'KBYI', 'KBZN', 'KCAE', 'KCAK', 'KCAR', 'KCBM', 'KCDC', 'KCDR', 'KCDS', 'KCEC', 'KCEF', 'KCGI', 'KCHA', 'KCHO', 'KCHS', 'KCID', 'KCKB', 'KCKV', 'KCLE', 'KCLL', 'KCLM', 'KCLT', 'KCMA', 'KCMH', 'KCMI', 'KCMX', 'KCNM', 'KCNU', 'KCNY', 'KCOD', 'KCOE', 'KCON', 'KCOS', 'KCOT', 'KCOU', 'KCPR', 'KCPS', 'KCRE', 'KCRG', 'KCRP', 'KCRQ', 'KCRW', 'KCSG', 'KCSM', 'KCSV', 'KCTB', 'KCUB', 'KCVG', 'KCWA', 'KCXO', 'KCXP', 'KCYS', 'KDAA', 'KDAB', 'KDAG', 'KDAL', 'KDAN', 'KDAY', 'KDBQ', 'KDCA', 'KDDC', 'KDEC', 'KDEN', 'KDET', 'KDFW', 'KDHN', 'KDHT', 'KDIJ', 'KDIK', 'KDLF', 'KDLH', 'KDLS', 'KDMA', 'KDMN', 'KDNL', 'KDOV', 'KDPA', 'KDRO', 'KDRT', 'KDSM', 'KDTW', 'KDUJ', 'KDVL', 'KDVT', 'KDYS', 'KEAR', 'KEAT', 'KEAU', 'KECG', 'KECP', 'KEDW', 'KEED', 'KEET', 'KEFD', 'KEFK', 'KEGE', 'KEGI', 'KEKN', 'KEKO', 'KEKS', 'KELD', 'KELM', 'KELP', 'KELY', 'KEND', 'KENV', 'KENW', 'KERI', 'KEUG', 'KEUL', 'KEVV', 'KEVW', 'KEWN', 'KEWR', 'KEYW', 'KFAF', 'KFAR', 'KFAT', 'KFAY', 'KFBG', 'KGPI', 'KFCS', 'KFDY', 'KFFO', 'KFHU', 'KFKL', 'KFLG', 'KFLL', 'KFLO', 'KFMH', 'KFMN', 'KFMY', 'KFNT', 'KFOD', 'KFOE', 'KFPR', 'KFRI', 'KFSD', 'KFSM', 'KFST', 'KFTK', 'KFTW', 'KFTY', 'KFVE', 'KFWA', 'KFXE', 'KFYV', 'KGBD', 'KGCC', 'KGCK', 'KGCN', 'KGDV', 'KGEG', 'KGFA', 'KGFK', 'KGFL', 'KGGG', 'KGGW', 'KGJT', 'KGKY', 'KGLD', 'KGLH', 'KGLS', 'KGMU', 'KGNV', 'KGON', 'KGPI', 'KGPT', 'KGRB', 'KGRF', 'KGRI', 'KGRK', 'KGRR', 'KGSB', 'KGSO', 'KGSP', 'KGTF', 'KGTR', 'KGUC', 'KGUP', 'KGUS', 'KGUY', 'KGWO', 'KGYY', 'KHBG', 'KHCR', 'KHDN', 'KHIB', 'KHIE', 'KHIF', 'KHIO', 'KHKS', 'KHKY', 'KHLG', 'KHLN', 'KHMN', 'KHND', 'KHOB', 'KHON', 'KHOP', 'KHOT', 'KHOU', 'KHPN', 'KHQM', 'KHRL', 'KHRO', 'KHRT', 'KHST', 'KHSV', 'KHTS', 'KHUF', 'KHUL', 'KHUM', 'KHUT', 'KHVR', 'KHYA', 'KHYR', 'KHYS', 'KIAB', 'KIAD', 'KIAG', 'KIAH', 'KICT', 'KIDA', 'KIFP', 'KILG', 'KILM', 'KILN', 'KIND', 'KINK', 'KINL', 'KINS', 'KINT', 'KINW', 'KIPL', 'KIPT', 'KISM', 'KISO', 'KISP', 'KITH', 'KIWA', 'KIWD', 'KIXD', 'KJAC', 'KJAN', 'KJAX', 'KJBR', 'KJCT', 'KJEF', 'KJFK', 'KJER', 'KJHW', 'KJLN', 'KJMS', 'KJST', 'KJZI', 'KLAF', 'KLAL', 'KLAN', 'KLAR', 'KLAS', 'KLAW', 'KLAX', 'KLBB', 'KLBE', 'KLBF', 'KLBL', 'KLCH', 'KLCK', 'KLEB', 'KLEE', 'KLEX', 'KLFI', 'KLFK', 'KLFT', 'KLGA', 'KLGB', 'KLGU', 'KLIT', 'KLLQ', 'KLMT', 'KLND', 'KLNK', 'KLNS', 'KLOZ', 'KLRD', 'KLRF', 'KLRU', 'KLSE', 'KLSF', 'KLSV', 'KLUF', 'KLUK', 'KLVK', 'KLVM', 'KLVS', 'KLWB', 'KLWS', 'KLWT', 'KLYH', 'KMAF', 'KMBG', 'KMBS', 'KMCB', 'KMCC', 'KMCE', 'KMCF', 'KMCI', 'KMCK', 'KMCN', 'KMCO', 'KMCW', 'KMDT', 'KMDW', 'KMEI', 'KMEM', 'KMER', 'KMEV', 'KMFE', 'KMFR', 'KMGE', 'KMGM', 'KMGW', 'KMHK', 'KMHR', 'KMHT', 'KMIA', 'KMIV', 'KMKE', 'KMKL', 'KMKT', 'KMLB', 'KMLC', 'KMLI', 'KMLS', 'KMLU', 'KMMH', 'KMMT', 'KMOB', 'KMOD', 'KMOT', 'KMPV', 'KMQY', 'KMRB', 'KMRY', 'KMSL', 'KMSN', 'KMSO', 'KMSP', 'KMSS', 'KMSY', 'KMTC', 'KMTH', 'KMTJ', 'KMTN', 'KMTW', 'KMUO', 'KMWH', 'KMXF', 'KMYL', 'KMYR', 'KNBC', 'KNBG', 'KNCA', 'KNEW', 'KNFG', 'KNFL', 'KNFW', 'KNGP', 'KNGU', 'KNHK', 'KNID', 'KNIP', 'KNJK', 'KNKT', 'KNKX', 'KNLC', 'KNMM', 'KNPA', 'KNQX', 'KNRB', 'KNSE', 'KNTD', 'KNTU', 'KNUC', 'KNUW', 'KNYG', 'KNYL', 'KNXP', 'KNZY', 'KOAJ', 'KOAK', 'KOFK', 'KOGB', 'KOGD', 'KOKC', 'KOLF', 'KOLM', 'KOLS', 'KOMA', 'KONP', 'KONT', 'KOPF', 'KORD', 'KORF', 'KORH', 'KOTH', 'KOTM', 'KOUN', 'KOWB', 'KOXR', 'KOZR', 'KPAE', 'KPAH', 'KPAM', 'KPBF', 'KPBG', 'KPBI', 'KPDK', 'KPDT', 'KPDX', 'KPEQ', 'KPGA', 'KPGD', 'KPGV', 'KPHF', 'KPHL', 'KPHX', 'KPIA', 'KPIB', 'KPIE', 'KPIH', 'KPIR', 'KPIT', 'KPKB', 'KPMD', 'KPNA', 'KPNC', 'KPNE', 'KPNS', 'KPOB', 'KPOU', 'KPQI', 'KPRB', 'KPRC', 'KPSC', 'KPSF', 'KPSM', 'KPSP', 'KPUB', 'KPUW', 'KPVD', 'KPVU', 'KPVW', 'KPWM', 'KPWT', 'KRAP', 'KRBG', 'KRBL', 'KRCA', 'KRDD', 'KRDG', 'KRDM', 'KRDR', 'KRDU', 'KRFD', 'KRIC', 'KRIL', 'KRIV', 'KRIW', 'KRKD', 'KRKS', 'KRME', 'KRNO', 'KROA', 'KROC', 'KROG', 'KROW', 'KRSL', 'KRST', 'KRSW', 'KRUT', 'KRVS', 'KRWI', 'KRWL', 'KRYY', 'KSAC', 'KSAF', 'KSAN', 'KSAT', 'KSAV', 'KSAW', 'KSBA', 'KSBD', 'KSBM', 'KSBN', 'KSBP', 'KSBY', 'KSCK', 'KSDF', 'KSDL', 'KSDY', 'KSEA', 'KSEZ', 'KSFB', 'KSFF', 'KSFO', 'KSGF', 'KSGJ', 'KSGU', 'KSGU', 'KSHR', 'KSHV', 'KSJC', 'KSJT', 'KSKA', 'KSKF', 'KSLC', 'KSLE', 'KSLI', 'KSLK', 'KSLN', 'KSME', 'KSMF', 'KSMN', 'KSMO', 'KSMX', 'KSNA', 'KSNS', 'KSNY', 'KSOA', 'KSPI', 'KSPS', 'KSRQ', 'KSSC', 'KSSF', 'KSTC', 'KSTL', 'KSTS', 'KSUA', 'KSUN', 'KSUS', 'KSUU', 'KSUX', 'KSWF', 'KSWO', 'KSYR', 'KSZL', 'KTCC', 'KTCL', 'KTCM', 'KTCS', 'KTEB', 'KTEX', 'KTIX', 'KTLH', 'KTMB', 'KTOI', 'KTOL', 'KTOP', 'KTPA', 'KTPH', 'KTRI', 'KTRK', 'KTRM', 'KTTD', 'KTTN', 'KTUL', 'KTUP', 'KTUS', 'KTVC', 'KTVL', 'KTWF', 'KTXK', 'KTYR', 'KTYS', 'KUAO', 'KUES', 'KUIN', 'KUKI', 'KUNV', 'KUTS', 'KVBG', 'KVCT', 'KVEL', 'KVGT', 'KVIS', 'KVLD', 'KVNY', 'KVPS', 'KVQQ', 'KVRB', 'KVTN', 'KWJF', 'KWMC', 'KWRB', 'KWRI', 'KWRL', 'KWWR', 'KWYS', 'KXNA', 'KXWA', 'KYIP', 'KYKM', 'KYNG', 'KZZV']
# list_of_airports = ['KVPS']

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
        metar_wind, metar_vis, metar_clouds_list = extractWeather(metar_and_taf, 0)
        print("Metar time: " + str(metar_time))
        print("Metar Wind: " + str(metar_wind))
        print("Metar Vis: " + str(metar_vis))
        print("Metar clouds:")
        k = 0
        for cloud in metar_clouds_list:
            print(metar_clouds_list[k])
            k+=1

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

        print("$$")

        current_day = int(taf_valid_start_day)
        current_hour = int(taf_valid_start_hour)
        end_day = int(taf_valid_end_day)
        end_hour = int(taf_valid_end_hour)


        print(len(taf_time_split))
        index = 0
        tempo_index = 0
        tempo_revert_flag = 0
        tempo_override_flag = 0
        flags_out = 0

        taf_wind, taf_vis, taf_clouds = extractWeather(taf_time_split, index)
        # DO NOT DELETE COMMENT
        # current_clouds = enumerateClouds(current_clouds)
        taf_clouds = ' '.join(taf_clouds)

        index += 1
        while (concat(current_day,current_hour)) != (concat(end_day, end_hour)):
            current_day_str, current_hour_str = zerofillDate(current_day, current_hour)

            if index + 1 < len(taf_time_split):
                if taf_time_split[tempo_index][:5] == "TEMPO" and \
                        current_day_str + current_hour_str == taf_time_split[tempo_index][11:15]:
                    if flags_out: print("TEMPO Down FLAG")

                    tempo_override_flag = 0
                    tempo_revert_flag = 1
                    tempo_index = 0
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
                    tempo_override_flag = 1
                    index += 2
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

            if tempo_override_flag:
                taf_wind, taf_vis, taf_clouds = tempo_wind, tempo_vis, tempo_clouds

            if tempo_revert_flag:
                taf_wind, taf_vis, taf_clouds = past_taf_wind, past_taf_vis, past_taf_clouds

            taf_time = str(enumerateDatetime(current_day_str + current_hour_str))
            print(taf_time + " | " + taf_wind + " | " + taf_vis + " | " + taf_clouds)

            current_hour += 1
            if current_hour > 23:
                current_hour = 0
                current_day += 1
        i+=1
        taffy_airport_list.append(airport)

print('------------')
print(i)
print(j)
print(taffy_airport_list)

t1 = time.time()
total = t1 - t0
print(total)

file1 = open("wxCompare_data","a")
file1.close()