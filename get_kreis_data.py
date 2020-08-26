from dataclasses import dataclass
from typing import Dict, List, Any

import requests
import sys
from requests import Response


@dataclass
class KreisInformation:
    kreis_name: str
    is_already_entered: bool
    number_of_new_cases: int
    bundesland: str

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

def preprocess_raw_data(kreis_names_raw: List[List[str]], new_cases_today_raw: List[List[str]], contributors_raw: List[List[str]]) -> List[KreisInformation]:
    new_cases_today: List[int] = [int(case_number[0].replace(" ", "")) for case_number in new_cases_today_raw]
    kreis_names: List[str] = [kreis_name[0] for kreis_name in kreis_names_raw]
    kreis_is_already_entered: List[bool] = [contributor[0] != "" for contributor in contributors_raw]
    bundeslaender: List[str] = get_bundeslaender(kreis_names)
    kreis_infos: List[KreisInformation] = [KreisInformation(name, is_entered, new_cases, bundesland)
                                           for name, is_entered, new_cases, bundesland in zip(kreis_names, kreis_is_already_entered, new_cases_today, bundeslaender)]
    return kreis_infos





API_KEY: str = sys.argv[1]
RISKLAYER_SPREADSHEET_ID = "1wg-s4_Lz2Stil6spQEYFdZaBEp8nWW26gVyfHqvcl8s"
parameters: Dict[str, str] = {"key": API_KEY}
spreadsheet_query_url: str = f'https://sheets.googleapis.com/v4/spreadsheets/{RISKLAYER_SPREADSHEET_ID}/values'

kreis_names_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!A6:A406', parameters).json()["values"]
new_cases_today_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!T6:T406', parameters).json()["values"]
contributors_raw: List[List[str]] = requests.get(f'{spreadsheet_query_url}/Haupt!S6:S406', parameters).json()["values"]

kreis_infos: List[KreisInformation] = preprocess_raw_data(kreis_names_raw, new_cases_today_raw, contributors_raw)


print(kreis_infos)
