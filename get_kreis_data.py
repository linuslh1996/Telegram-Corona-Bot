from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List

import requests
import sys

from psycopg2.sql import SQL, Literal

from database import PostgresDatabase


@dataclass
class KreisInformation:
    id: int
    kreis_name: str
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

postgres_db: PostgresDatabase = PostgresDatabase("postgres", "admin", "localhost", "5432", "coronabot")
names = postgres_db.get_column_names("kreise")

today: datetime.date = datetime.date(datetime.now())
result = postgres_db.get(SQL("SELECT * FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                             "WHERE k.bundesland = 'Schleswig-Holstein' AND f.date = {today}").format(today=Literal(today)))



API_KEY: str = sys.argv[1]
RISKLAYER_SPREADSHEET_ID = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"
parameters: Dict[str, str] = {"key": API_KEY}
spreadsheet_query_url: str = f'https://sheets.googleapis.com/v4/spreadsheets/{RISKLAYER_SPREADSHEET_ID}/values'



kreis_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!A6:A406', parameters).json()["values"]
new_cases_today_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!T6:T406', parameters).json()["values"]
contributors_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!S6:S406', parameters).json()["values"]
links_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!R6:R406', parameters).json()["values"]

kreis_infos: List[KreisInformation] = preprocess_raw_data(kreis_names_raw, new_cases_today_raw, contributors_raw, links_raw)
as_table_format: List[Dict] = [{"kreis_id": kreis.id, "fallzahlen": kreis.number_of_new_cases,
                                "link": kreis.link, "is_already_entered": kreis.is_already_entered, "date": datetime.date(datetime.now())} for kreis in kreis_infos]
postgres_db.upsert("fallzahlen", as_table_format)
print([kreis_info for kreis_info in kreis_infos if kreis_info.is_already_entered])
