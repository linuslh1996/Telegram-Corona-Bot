# Domain-Specific Names stay in German for (hopefully) better readability
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup, ResultSet
from psycopg2.sql import SQL
from requests import Response
from selenium.webdriver.firefox.options import Options

import data_modules.helper_functions as help

from selenium import webdriver
from data_modules.database import PostgresDatabase


@dataclass
class KreisInformation:
    kreis_id: int
    kreis: str
    is_already_entered: bool
    number_of_new_cases: int
    link: str
    bundesland: Optional[str]
    date: datetime.date

def get_new_data(api_key: str) -> List[KreisInformation]:
    kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw = _get_from_API(api_key)
    kreis_infos: List[KreisInformation] = _preprocess_raw_data(kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw)
    return kreis_infos

def get_all_kreise(postgres_db: PostgresDatabase) -> List[str]:
    sql: SQL = SQL("SELECT kreis FROM kreise")
    results: List[str] = postgres_db.get(sql).convert_to_primitive_type(str)
    return results


def get_all_bundeslaender(postgres_db: PostgresDatabase) -> List[str]:
    sql: SQL = SQL("SELECT DISTINCT bundesland FROM kreise")
    results = postgres_db.get(sql).convert_to_primitive_type(str)
    return results

def _get_from_API(api_key: str) -> Tuple[List[List[str]], List[List[str]], List[List[str]], List[List[str]]]:
    # Define Spreadsheet Url
    RISKLAYER_SPREADSHEET_ID = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"
    parameters: Dict[str, str] = {"key": api_key}
    spreadsheet_query_url: str = f'https://sheets.googleapis.com/v4/spreadsheets/{RISKLAYER_SPREADSHEET_ID}/values'

    # Query Data
    kreis_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!A6:A406', parameters).json()["values"]
    new_cases_today_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!T6:T406', parameters).json()["values"]
    contributors_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!S6:S406', parameters).json()["values"]
    links_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!R6:R406', parameters).json()["values"]

    return kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw

def _get_from_scraping() -> Tuple[List[List[str]], List[List[str]], List[List[str]], List[List[str]]]:
    # Init Session
    options = Options()
    options.headless = True
    URL: str = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTB9XnOufMUQ4Plp6JWi2UAoND8jvBH2oH_vPQGIw5btYHqnSXxeVnpCz-1cwgjNpI48tqDgs51kO7n/pubhtml#"
    driver: webdriver.Firefox = webdriver.Firefox(options=options)
    driver.set_page_load_timeout(30)
    driver.get(URL)
    html: str  = driver.page_source
    driver.quit()

    # Init BeautifulSoup
    html_page: BeautifulSoup = BeautifulSoup(html)
    all_rows: ResultSet = html_page.findAll("tr")
    only_valid_rows = [row for row in all_rows if _is_valid_row(row)]

    # Push Results
    kreis_names_raw: List[List[str]] = []
    new_cases_today_raw: List[List[str]] = []
    contributors_raw: List[List[str]] = []
    links_raw: List[List[str]] = []
    for row in only_valid_rows:
        kreis_names_raw.append([str(row.findAll("td")[0].text)])
        new_cases_today_raw.append([str(row.findAll("td")[20].text)])
        contributors_raw.append([str(row.findAll("td")[19].text)])
        links_raw.append([str(row.findAll("td")[18].text)])
    return kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw

def _is_valid_row(row) -> bool:
    if len(row.findAll("td")) < 5:
        return False
    if "kreis" not in str(row.findAll("td")[2].text).lower():
        return False
    if "%" not in str(row.findAll("td")[5].text).lower():
        return False
    return True

def _preprocess_raw_data(kreis_names_raw: List[List[str]], new_cases_today_raw: List[List[str]], contributors_raw: List[List[str]], links_raw: List[List[str]]) -> List[KreisInformation]:
    new_cases_today: List[int] = [int(case_number[0].replace(" ", "")) if case_number[0] != "" and case_number[0] != "S"
                                        else 0 
                                        for case_number in new_cases_today_raw ]
    kreis_names: List[str] = [kreis_name[0] for kreis_name in kreis_names_raw]
    links: List[str] = [link[0] for link in links_raw]
    kreis_is_already_entered: List[bool] = [len(contributor) != 0 and not "Vorläufig" in contributor
                                            and not "" in contributor for contributor in contributors_raw]
    already_entered_in_correct_length = kreis_is_already_entered + [False for remaining in range(len(kreis_is_already_entered), 401)] # Google has a really weird (imo awful) way of returning values. If you request the value for 400 row, and the last 200 rows are empty, google will only return the first 200 rows.
    current_date: datetime.date = datetime.date(help.get_current_german_time())
    kreis_infos: List[KreisInformation] = [KreisInformation(i, name, is_entered, new_cases, link, None, current_date)
                                           for i, (name, is_entered, new_cases, link) in enumerate(zip(kreis_names, already_entered_in_correct_length, new_cases_today, links))]
    return kreis_infos

