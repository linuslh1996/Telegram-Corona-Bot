from datetime import datetime, timedelta
from typing import List, Dict

import requests

from database import PostgresDatabase
from risklayer import KreisInformation


# Collects Functions that were used to fill the database, but are not needed elsewise


# Relies heavily on the knowledge about how the risklayer table is set up. Basically the kreise are sorted by Bundesland
def get_bundeslaender(kreis_names: List[str]) -> List[str]:
    order_of_bundeslaender: List[str] = ["Schleswig-Holstein", "Hamburg", "Niedersachsen", "Bremen", "Nordrhein-Westfalen", "Hessen",
                                         "Rheinland-Pfalz", "Baden-Württemberg", "Bayern", "Saarland", "Berlin", "Brandenburg", "Mecklenburg-Vorpommern",
                                         "Sachsen", "Sachsen-Anhalt", "Thüringen"]
    current_bundesland_index: int = 0
    previous_kreisname = ""
    without_weird_german_alphabet: List[str] = [kreis_name.replace("ö", "o").replace("ä", "a").replace("ü", "u") for kreis_name in kreis_names]
    bundeslaender: List[str] = []
    for kreis_name in without_weird_german_alphabet:
        if kreis_name < previous_kreisname: # If we had for instance "Wolfsburg" and after it "Bremen", we know that "Bremen" belongs to a new Bundesland
            current_bundesland_index += 1
        bundeslaender.append(order_of_bundeslaender[current_bundesland_index])
        previous_kreisname = kreis_name
    return bundeslaender


def insert_old_data(postgres_db: PostgresDatabase, api_key: str):

    # Define Spreadsheet Url

    RISKLAYER_SPREADSHEET_ID = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"
    parameters: Dict[str, str] = {"key": api_key}
    spreadsheet_query_url: str = f'https://sheets.googleapis.com/v4/spreadsheets/{RISKLAYER_SPREADSHEET_ID}/values'

    # Query Data

    kreis_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Kreise!C4:C404', parameters).json()["values"]
    kreis_names: List[str] = [kreis[0] for kreis in kreis_names_raw]
    bundesland_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Kreise!D4:D404', parameters).json()["values"]
    bundesland_names: List[str] = [bundesland[0] for bundesland in bundesland_names_raw]
    columns: List[str] = ["AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH"]
    case_history: List[List[int]] = []
    for column in columns:
        cases_of_day_raw: List[List[int]] = requests.get(f'{spreadsheet_query_url}/Kreise!{column}4:{column}404', parameters).json()["values"]
        cases_of_day: List[int] = [int(case[0]) for case in cases_of_day_raw]
        case_history.append(cases_of_day)

    # Zip Data

    normalised = list(zip(kreis_names, bundesland_names, zip(*case_history)))

    # Sort (To get the Id right)

    order_of_bundeslaender: List[str] = ["Schleswig-Holstein", "Hamburg", "Niedersachsen", "Bremen", "Nordrhein-Westfalen", "Hessen",
                                         "Rheinland-Pfalz", "Baden-Württemberg", "Bayern", "Saarland", "Berlin", "Brandenburg", "Mecklenburg-Vorpommern",
                                         "Sachsen", "Sachsen-Anhalt", "Thüringen"]
    ordered_by_name: List = sorted(normalised, key=lambda kreis_info: kreis_info[0])
    ordered_by_bundesland: List = sorted(ordered_by_name, key=lambda kreis_info: order_of_bundeslaender.index(kreis_info[1]))

    # Insert Into Db

    last_date: datetime = datetime(year=2020, month=8, day=29)
    for id,entry in enumerate(ordered_by_bundesland):
        for i in range(0,7):
            date_of_day = last_date - timedelta(days=i)
            cases_of_day: int = entry[2][i]
            cases_day_before: int = entry[2][i+1]
            difference: int = cases_of_day - cases_day_before
            as_db_row = {"kreis_id": id, "number_of_new_cases": difference,
                                    "link": "", "is_already_entered": True,
                                    "date": date_of_day}
            print(as_db_row)
            postgres_db.insert("fallzahlen", [as_db_row])


