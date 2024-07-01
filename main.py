import asyncio
import helper_funtions
import time

start = time.time()

# Define seasons to get the matches of each matchday
all_seasons_list = [f"/{year}-{str(year + 1)[-2:]}/" for year in range(1965, 2024)]
spieldaten_seasons = [f"/{year}-{str(year + 1)[-2:]}/" for year in range(2014, 2024)]
first_to_season = ['/1963-64/', '/1964-65/']
test_seasons = [f"/{year}-{str(year + 1)[-2:]}/" for year in range(2023, 2024)]

# Define a list with proxies to rotate the proxies for the requests
proxy_list = helper_funtions.create_proxy_list()


# For each season in the list, the matches of all match days are extracted.
# Schema: id, season, home team, away team, fulltime home goals, fulltime away goals, halftime home goals, halftime away goals, matchinfo_url
def run_bundesliga_matchdayresult_scrapping(proxies, all_season_list):
    proxies = proxies
    for season in all_season_list:
        url_list = helper_funtions.create_season_url_list(season)
        asyncio.run(
            helper_funtions.run_request(helper_funtions.extract_bundesliga_matchday_results, url_list, proxies[0]))
        proxies = helper_funtions.list_move_first_to_end(proxy_list)
        print(f"Die Saison {season} wurde gespeichert.")


# The links from a CSV file are loaded into a list. Another list is created,
# which subdivides the URL list for the proxy rotation again. For each sublist, all urls are called and the
# match information is extracted.
# Schema: id, kickoff,home team, away team, stadium name, viewer, stadium city, referee name, referee town
# Function to merge results and infos is not included
def run_bundesliga_matchinfo_scrapping():
    csv_data = helper_funtions.load_matchinfo_urls("PATH")
    url_list_cutted = [csv_data[i:i + 10] for i in range(0, len(csv_data), 10)]

    for i in range(len(url_list_cutted)):
        asyncio.run(helper_funtions.matchinfo_request(url_list_cutted[i]))
        time.sleep(1)
        print("Die " + str(i) + ". Liste von " + str(len(url_list_cutted)) + " Listen wurde gespeichert.")
    print(f"Die Spielinformationen wurden gespeichert.")


# End of Skript
if __name__ == "__main__":
    run_bundesliga_matchdayresult_scrapping(proxy_list, all_seasons_list)
    run_bundesliga_matchinfo_scrapping()

    end = time.time()
    print("\n\nDas Skript hatte eine Laufzeit von: " + str(round(end - start, 2)) + " Sekunden.")
