# Domain-Specific Names stay in German for (hopefully) better readability
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict

import requests

from database import PostgresDatabase


@dataclass
class KreisInformation:
    kreis_id: int
    kreis: str
    is_already_entered: bool
    number_of_new_cases: int
    link: str

def preprocess_raw_data(kreis_names_raw: List[List[str]], new_cases_today_raw: List[List[str]], contributors_raw: List[List[str]], links_raw: List[List[str]]) -> List[KreisInformation]:
    new_cases_today: List[int] = [int(case_number[0].replace(" ", "")) for case_number in new_cases_today_raw]
    kreis_names: List[str] = [kreis_name[0] for kreis_name in kreis_names_raw]
    links: List[str] = [link[0] for link in links_raw]
    kreis_is_already_entered: List[bool] = [len(contributor) != 0 for contributor in contributors_raw]
    kreis_infos: List[KreisInformation] = [KreisInformation(i, name, is_entered, new_cases, link)
                                           for i, (name, is_entered, new_cases, link) in enumerate(zip(kreis_names, kreis_is_already_entered, new_cases_today, links))]
    return kreis_infos

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
    kreis_infos: List[KreisInformation] = preprocess_raw_data(kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw)
    return kreis_infos

def update_data(database: PostgresDatabase, api_key: str):
    kreis_infos: List[KreisInformation] = get_new_data(api_key)
    as_table_format: List[Dict] = [{"kreis_id": kreis.kreis_id, "number_of_new_cases": kreis.number_of_new_cases,
                                    "link": kreis.link, "is_already_entered": kreis.is_already_entered,
                                    "date": datetime.date(datetime.now())} for kreis in kreis_infos]
    database.upsert("fallzahlen", as_table_format)