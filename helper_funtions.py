from seleniumwire import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from requests_html import AsyncHTMLSession
import asyncio
import nest_asyncio
import os
import aiohttp

# Settings that are necessary for the code to run
setattr(asyncio.sslproto._SSLProtocolTransport, "_start_tls_compatible", True)
nest_asyncio.apply()


# Create a list of https proxies
def create_proxy_list():
    username = 'USERNAME'
    password = 'PASSWORD'
    ip_port_list = ['isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port',
                    'isp.oxylabs.io:port']
    proxy_list = [
        {"https": f"https://{username}:{password}@{ip_port}"}
        for ip_port in ip_port_list
    ]
    return proxy_list


# Create an instance of the Chrome WebDriver [Out of time because requests_html is used instead of selenium]
def initialize_webdriver(webdriver_path, proxy):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    options = {
        "proxy": proxy
    }
    service = webdriver.Chrome
    driver = webdriver.Chrome(webdriver_path, options=chrome_options, seleniumwire_options=options)

    return driver


# Checks whether an element is in the list and duplicates it.
def duplicate_list_element(liste, element):
    i = 0
    while i < len(liste):
        if liste[i] == element:
            liste.insert(i + 1, element)
            i += 1  # Überspringe das duplizierte Element, um Endlosschleifen zu vermeiden
        i += 1
    return liste


# Returns all HTML elements with the specified class name
def parsing_page_source(html_page_source):
    soup = BeautifulSoup(html_page_source, "html.parser")
    all_matches_of_matchday = soup.find_all(class_="kick__v100-gameList kick__module-margin")

    return all_matches_of_matchday


# Combine the date with the respective games on this date in a dictionary
def get_kicker_matchday_dates(soup_object):
    result_dictionary = []

    for i in range(len(soup_object)):
        date = soup_object[i].find("div", class_="kick__v100-gameList__header").text.strip(
            "\r\n                        ")
        matches = len(soup_object[i].find_all("a", class_="kick__v100-scoreBoard kick__v100-scoreBoard--standard"))
        result_dictionary.append([date, matches])

    return result_dictionary


# Sorts the teams into home and away team
def sort_home_away_teams(short_team_names_list):
    home_teams = [short_team_names_list[i] for i in range(0, len(short_team_names_list), 2)]
    away_teams = [short_team_names_list[i] for i in range(1, len(short_team_names_list), 2)]

    return {"Home": home_teams,
            "Away": away_teams}


# Extracts all goals scored in a list
def get_goals_per_matchday(count_matchdays, index):
    matchday_goals = [div.text for div in
                      count_matchdays[index].find_all("div", class_=lambda class_value: class_value and (
                              "kick__v100-scoreBoard__scoreHolder__score" in class_value or
                              "kick__v100-scoreBoard__scoreHolder__text" in class_value))]
    matchday_goals = duplicate_list_element(matchday_goals, "abgbr.")
    matchday_goals = duplicate_list_element(matchday_goals, "gew.")
    matchday_goals = duplicate_list_element(matchday_goals, "annull.")

    return matchday_goals


# Combines the date with the respective teams that played on this date in a list
def combine_dates_with_teams(all_matches_of_matchday):
    result = []
    for i in range(len(all_matches_of_matchday)):
        short_names = [div.text for div in
                       all_matches_of_matchday[i].find_all("div", class_="kick__v100-gameCell__team__shortname")]
        sorted_names = sort_home_away_teams(short_names)
        dates_and_matches_count = get_kicker_matchday_dates(all_matches_of_matchday)
        result.append([dates_and_matches_count[i][0], sorted_names])

    return result


# Combines the date with the respective goals scored on this date in a list
def combine_dates_with_goals(all_matches_of_matchday):
    result = []
    for i in range(len(all_matches_of_matchday)):
        dates_and_matches_count = get_kicker_matchday_dates(all_matches_of_matchday)
        matchday_goals = get_goals_per_matchday(all_matches_of_matchday, i)
        result.append([dates_and_matches_count[i][0], matchday_goals])

    return result


# Combines the date with the respective urls of the games that were played on this date in a list
def combine_dates_with_urls(all_matches_of_matchday):
    result = []
    for i in range(len(all_matches_of_matchday)):
        analyse_url = [div.get("href").replace("schema", "spielinfo").replace("analyse", "spielinfo") for div in
                       all_matches_of_matchday[i].find_all("a",
                                                           class_=lambda class_value: class_value and (
                                                                   "kick__v100-gameList__gameRow__stateCell__indicator kick__v100-gameList__gameRow__stateCell__indicator--schema" in class_value or
                                                                   "kick__v100-scoreBoard kick__v100-scoreBoard--standard kick__v100-scoreBoard--videoincl" in class_value)
                                                           )]
        dates_and_matches_count = get_kicker_matchday_dates(all_matches_of_matchday)
        result.append([dates_and_matches_count[i][0], list(dict.fromkeys(analyse_url))])

    return result


# CSV files are created with 8-9 entries (matches) per match day.
# Schema: id, season, home team, away team, fulltime home goals, fulltime away goals, halftime home goals, halftime away goals, matchinfo_url
def build_matchday_dataset(all_matches_of_matchday, season, matchday_index):
    matchday_teams = combine_dates_with_teams(all_matches_of_matchday)
    matchday_goals = combine_dates_with_goals(all_matches_of_matchday)
    matchday_urls = combine_dates_with_urls(all_matches_of_matchday)
    # fieldnames = ["Season", "Date", "Home Team", "Away Team", "FT Home Goals", "FT Away Goals", "HT Home Goals",
    #              "HT Away Goals", "URL Analyse"]
    season = season.replace("/", "-")
    filepath_csv = "_" + str(matchday_index) + ".csv"
    filepath_csv_urls = "_" + str(matchday_index) + "_urls.csv"
    path = "C:/Users/Luis/PycharmProjects/kicker_webcrawler/DATA/" + season

    data = []
    matchinfo_urls = []

    for i in range(len(matchday_teams)):
        for n in range(len(matchday_teams[i][1]["Home"])):
            try:
                id_date = matchday_teams[i][0].split(", ", 2)[1].replace(".", "")
                home_id = matchday_teams[i][1]["Home"][n][:4].replace("'", "")
                away_id = matchday_teams[i][1]["Away"][n][:4].replace("'", "")

                data_entry = {"id": f"{id_date}{home_id}{away_id}".replace("'", ""),
                              "Season": season, "Date": matchday_teams[i][0],
                              "Home Team": matchday_teams[i][1]["Home"][n],
                              "Away Team": matchday_teams[i][1]["Away"][n],
                              "FT Home Goals": matchday_goals[i][1][0],
                              "FT Away Goals": matchday_goals[i][1][1],
                              "HT Home Goals": matchday_goals[i][1][2],
                              "HT Away Goals": matchday_goals[i][1][3],
                              "URL Analyse": matchday_urls[i][1][n]}
                matchday_info_entry = {
                    "id": f"{id_date}{home_id}{away_id}".replace("'", ""),
                    "matchinfo_url": f"https://www.kicker.de{matchday_urls[i][1][n]}",
                    "season": season,
                    "matchday_index": matchday_index
                }
                data.append(data_entry)
                matchinfo_urls.append(matchday_info_entry)
                del matchday_goals[i][1][:4]
            except IndexError as e:
                id_date = matchday_teams[i][0].split(", ", 2)[1].replace(".", "")
                home_id = matchday_teams[i][1]["Home"][n][:4].replace("'", "")
                away_id = matchday_teams[i][1]["Away"][n][:4].replace("'", "")
                data_entry = {"id": f"{id_date}{home_id}{away_id}".replace("'", ""),
                              "Season": season, "Date": matchday_teams[i][0],
                              "Home Team": matchday_teams[i][1]["Home"][n],
                              "Away Team": matchday_teams[i][1]["Away"][n],
                              "FT Home Goals": matchday_goals[i][1][0],
                              "FT Away Goals": matchday_goals[i][1][1],
                              "HT Home Goals": matchday_goals[i][1][2],
                              "HT Away Goals": matchday_goals[i][1][3],
                              "URL Analyse": ""}
                matchday_info_entry = {
                    "id": f"{id_date}{home_id}{away_id}".replace("'", ""),
                    "matchinfo_url": f"https://www.kicker.de{matchday_urls[i][1][n]}",
                    "season": season,
                    "matchday_index": matchday_index

                }
                data.append(data_entry)
                matchinfo_urls.append(matchday_info_entry)
                del matchday_goals[i][1][:4]
                print("Es wurde kein Link für den Eintrag gefunden")

    df = pd.DataFrame(data)
    df2 = pd.DataFrame(matchinfo_urls)
    df.to_csv(path + filepath_csv, encoding="utf-8")
    df2.to_csv(path + filepath_csv_urls, encoding="utf-8")
    print("Die CSV-Datei zu: " + season + "_" + str(matchday_index) + " wurde erfolgreich gespeichert.")


# Places the first element of a list at the end of the list
def list_move_first_to_end(lst):
    if len(lst) > 0:
        first_element = lst.pop(0)  # Entferne und speichere das erste Element
        lst.append(first_element)  # Füge das gespeicherte Element ans Ende der Liste
    return lst


# Creates a list with urls for all match days per season
def create_season_url_list(season):
    result_list = []
    all_seasons_list = [f"/{year}-{str(year + 1)[-2:]}/" for year in range(2021, 2024)]
    # thirty_matchday_seasons = [f"/{year}-{str(year + 1)[-2:]}/" for year in range(1963, 1965)]
    for n in range(1, 35):
        result_list.append(f"https://www.kicker.de/bundesliga/spieltag{season}{n}")
    return result_list


# Creates a list with paths to all csv files in a directory with a specific suffix
def find_csv_files(directory, suffix='.csv'):
    """
    Find all CSV files in the given directory that end with the specified suffix.

    :param directory: Directory to search for CSV files.
    :param suffix: Suffix that the target CSV files should end with.
    :return: List of file paths matching the criteria.
    """
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(suffix):
                csv_files.append(os.path.join(root, file))
    return csv_files


# Saves all urls from a CSV file in a list
def extract_urls_from_csv(file_path):
    """
    Extract URLs from the second column of the CSV file.

    :param file_path: Path to the CSV file.
    :return: List of URLs.
    """
    df = pd.read_csv(file_path)
    df_cut = df[df.columns[-1]]
    if df_cut.name == "URL Analyse":
        urls = df_cut.tolist()
        for i in range(len(urls)):
            if urls[i].endswith("schema"):
                urls[i] = urls[i].replace("schema", "spielinfo")

        return urls


# Loads all urls from multiple csv files
def load_matchinfo_urls(path):
    csv_files = find_csv_files(path)
    all_urls = []

    for csv_file in csv_files:
        try:
            urls = extract_urls_from_csv(csv_file)
            all_urls.extend(urls)
        except TypeError as e:
            print("Die Daten aus der Datei mit dem Pfad: *** " + csv_file + " *** konnte nicht gefunden werden.")
            continue
    return all_urls


def match_info_urls_per_season(url_list:list, season:str):
    season_urls = []
    for url_element in url_list:
        if url_element[3] == season:
            season_urls.append([url_element[1]])

    return season_urls


# Modifies a string
def pretty_string(string):
    return string.replace("\n", "").replace("\r", "").replace("\t", "")


# Checks whether a variable is a none value
def check_nv(var_to_check):
    if var_to_check is not None:
        return var_to_check
    else:
        return ""


# Dataentry for the CSV file are created with 1 entry per match.
# Schema: id, kickoff,home team, away team, stadium name, viewer, stadium city, referee name, referee town
def create_matchday_info_csv(soup):
    # Extract kickoff, stadium, viewer
    first_info_box = soup.find(class_="kick__gameinfo__item kick__gameinfo__item--game-preview")
    for tag in ["br", "strong"]:
        for element in first_info_box.find_all(tag):
            element.decompose()
    game_info_blocks = first_info_box.find_all(class_="kick__gameinfo-block")

    #Elements
    #Stadium
    stadium_name = game_info_blocks[1].find("a").get_text()
    stadium_name = check_nv(stadium_name)


    stadium_city = pretty_string(game_info_blocks[1].find("p").get_text().replace("(", "").replace(")", "")).strip(
        stadium_name).strip(" ")
    stadium_city = check_nv(stadium_city)


    # Date
    date = pretty_string(game_info_blocks[0].find("p").get_text()).strip(" ").replace('"', '')
    date = check_nv(date)
    date_for_id = date[2:].split(",", 1)[0].replace(".", "")

    # Viewer
    zuschauer_int = pretty_string(
        first_info_box.find(class_="kick__gameinfo-block kick__tabular-nums").find("p").get_text().strip(" ").replace(
            ".", "").replace(" (ausverkauft)", ""))
    zuschauer_int = check_nv(zuschauer_int)
    if isinstance(zuschauer_int, int):
        zuschauer_int = int(zuschauer_int)
    else:
        zuschauer_int = check_nv(zuschauer_int)

    # Extract referee information
    second_info_box = soup.find(class_="kick__gameinfo__item kick__gameinfo__item--game-review")

    # Referee name
    referee = second_info_box.find_all("p")[0].get_text().split(" ", 2)[0] + " " + \
              second_info_box.find_all("p")[0].get_text().split(" ", 2)[1]
    referee = check_nv(referee)
    #Referee town
    referee_town = second_info_box.find_all("p")[0].get_text().split(" ", 2)[2].replace("(", "").replace(")", "")
    referee_town = check_nv(referee_town)

    # Extract Teams
    third_info_box = soup.find(class_="kick__modul__item")
    team_names = [check_nv(third_info_box.find_all("div", class_="kick__v100-gameCell__team__shortname")[i].get_text().strip(" "))
                  for i in range(len(third_info_box.find_all("div", class_="kick__v100-gameCell__team__shortname")))]
    home_team = team_names[0]
    home_id = home_team[:4]
    away_team = team_names[1]
    away_id = away_team[:4]

    # Define Primary Key
    id = f"{date_for_id}{home_id}{away_id}".replace("'", "")
    id = check_nv(id)

    # Create CSV
    data_entry = {
        "id": id,
        "kickoff": date,
        "home_team": home_team,
        "away_team": away_team,
        "stadium": stadium_name,
        "viewer": zuschauer_int,
        "city": stadium_city,
        "referee": referee,
        "referee hometown": referee_town
    }
    return data_entry


# ASYNCIO: Get a bs4 object and build the result dataset
async def extract_bundesliga_matchday_results(session, url, proxy):
    r = await session.get(url, proxies=proxy)
    r.html.arender(timeout=10)
    soup = r.text

    season = url[42:49]
    matchday_index = int(url[50:])

    all_matches = parsing_page_source(soup)
    build_matchday_dataset(all_matches, season, matchday_index)


# ASYNCIO: Get a bs4 object and return it
async def fetch(session, url, index, proxy):
    i = index

    if i % 10 == 0:
        #proxy_list = list_move_first_to_end(proxy_list)
        print("Der Proxy wurde nach der " + str(i) + ". Anfrage gewechselt.")
        async with session.get("https://www.kicker.de" + url, proxy=proxy["https"]) as response:
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            #await asyncio.sleep(1)
            print(f"Abfrage für {i} gesendet mit {proxy['https']}")
            return soup
    else:
        async with session.get("https://www.kicker.de" + url, proxy=proxy["https"]) as response:
            html_content = await response.text()
            soup = BeautifulSoup(html_content, 'html.parser')
            #print(f"Abfrage für {i} gesendet")
            print(f"Abfrage für {i} gesendet mit {proxy['https']}")
            return soup


# ASYNCIO: Run the requests for the match results in a loop
async def run_request(func, url_list, proxy):
    s = AsyncHTMLSession()
    tasks = (func(s, url, proxy) for url in url_list)
    output = await asyncio.gather(*tasks)
    await s.close()
    return output


# Run the requests for the match infos in a loop
async def matchinfo_request(url_list):
    data = []
    error_count = 0
    request_count = 0
    proxy_list = create_proxy_list()
    proxy_counter = 0
    async with aiohttp.ClientSession(trust_env=True) as session:
        tasks = []
        for url in range(len(url_list)):
            if proxy_counter <= 9:
                tasks.append(fetch(session, url_list[url], url, proxy_list[proxy_counter]))
                request_count += 1
                proxy_counter += 1
            else:
                proxy_counter = 0
        objects = await asyncio.gather(*tasks)

    for n in range(len(objects)):
        try:
            data_entry = create_matchday_info_csv(objects[n])
            data.append(data_entry)
            print("Daten wurden für den Index: " + str(n) + " gefunden.")
        except AttributeError as e:
            print(e)
            error_count += 1
            continue

    df = pd.DataFrame(data)
    df.to_csv("C:\\Users\\Luis\PycharmProjects\\kicker_webcrawler\\DATA\\" + "matchinfos_new_extend.csv", mode="a", header=False)
    return print("Es wurden von " + str(len(url_list)) + " Spielen die Spielinformationen gespeichert.\n"
                                                         "Für " + str(error_count) + "konnten keine "
                                                                                     "Spielinformationen gespeichert "
                                                                                     "werden.\n Es wurden " + str(request_count) + " Abfragen durchgeführt.")
