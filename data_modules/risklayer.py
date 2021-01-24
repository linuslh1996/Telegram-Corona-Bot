# Domain-Specific Names stay in German for (hopefully) better readability
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional

import requests
from psycopg2.sql import SQL
import data_modules.helper_functions as help

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
    # Define Spreadsheet Url
    RISKLAYER_SPREADSHEET_ID = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"
    parameters: Dict[str, str] = {"key": api_key}
    spreadsheet_query_url: str = f'https://sheets.googleapis.com/v4/spreadsheets/{RISKLAYER_SPREADSHEET_ID}/values'
    # Query Data
    kreis_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!A6:A406', parameters).json()["values"]
    new_cases_today_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!T6:T406', parameters).json()["values"]
    contributors_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!S6:S406', parameters).json()["values"]
    links_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!R6:R406', parameters).json()["values"]
    # Convert Data
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


def _preprocess_raw_data(kreis_names_raw: List[List[str]], new_cases_today_raw: List[List[str]], contributors_raw: List[List[str]], links_raw: List[List[str]]) -> List[KreisInformation]:
    new_cases_today: List[int] = [int(case_number[0].replace(" ", "")) for case_number in new_cases_today_raw]
    kreis_names: List[str] = [kreis_name[0] for kreis_name in kreis_names_raw]
    links: List[str] = [link[0] for link in links_raw]
    kreis_is_already_entered: List[bool] = [len(contributor) != 0 and not "Vorläufig" in contributor
                                            for contributor in contributors_raw]
    already_entered_in_correct_length = kreis_is_already_entered + [False for remaining in range(len(kreis_is_already_entered), 401)] # Google has a really weird (imo awful) way of returning values. If you request the value for 400 row, and the last 200 rows are empty, google will only return the first 200 rows.
    current_date: datetime.date = datetime.date(help.get_current_german_time())
    kreis_infos: List[KreisInformation] = [KreisInformation(i, name, is_entered, new_cases, link, None, current_date)
                                           for i, (name, is_entered, new_cases, link) in enumerate(zip(kreis_names, already_entered_in_correct_length, new_cases_today, links))]
    return kreis_infos

