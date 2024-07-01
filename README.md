# Bundesliga Web Scraper

Welcome to the Bundesliga Web Scraper repository! This project is a comprehensive web scraper that has collected Bundesliga match results from every season between 1963 and 2024. The data includes essential details such as match dates, teams, and results. Additionally, some matches feature extended information, though not all due to the varying structures of different URLs, which presented challenges in parsing the HTML elements using BeautifulSoup.

## Project Overview

### Data Collected
- **Match Date**: The date on which the match was played.
- **Teams**: The names of the teams that competed.
- **Results**: The final score of the match.
- **Additional Information**: For some matches, additional details are provided.

### Challenges
While scraping the data, some issues arose:
- **Inconsistent URL Structures**: Different URL structures led to difficulties in locating HTML elements, resulting in incomplete data for some matches.
- **Proxy Rotation**: To avoid being blocked, a proxy rotation with 10 different IP addresses was implemented during the requests.

### Notes
- **No Analysis or Transformation Functions**: This repository strictly contains the web scraping scripts and the raw data collected. Functions for data analysis or transformation are not included.
- **Spaghetti Code Warning**: As this was my first foray into web scraping, the code quality might resemble a delicious plate of spaghetti. Apologies for any inconvenience!
