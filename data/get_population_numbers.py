import os
from dataclasses import dataclass
from typing import List, Dict
import json
import pandas as pd
from pandas import DataFrame
from pathlib import Path

from data_modules.database import PostgresDatabase
from data_modules.scheme import Kreis

# Script that was used to add population data to the kreise


@dataclass
class KreisErweiterung:
    name: str
    type_of_kreis: str
    population_number: int

# Get DB Access
API_KEY: str = os.environ["API_KEY"]
TELEGRAM_TOKEN: str = os.environ["TELEGRAM_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
postgres_db: PostgresDatabase = PostgresDatabase(DATABASE_URL)

# Load kreis.json (that contains the population data for each kreis)
file: Path = Path("kreis.json")
kreis_info: List[Dict] = json.load(file.open())
kreis_typed: List[KreisErweiterung] = [KreisErweiterung(name=info["fields"]["gen"], type_of_kreis=info["fields"]["bez"], population_number=info["fields"]["ewz"])
              for info in kreis_info]

# Get the Kreise that are currently stored in the DB
kreise: List[Kreis] = postgres_db.getAll("kreise").convert_rows_to(Kreis)
kreis_and_town_both: List[str] = [kreis.kreis.split("_")[0] for kreis in kreise
                                  if "_Kreis" in kreis.kreis]

# Merge Both Infos (Goal is to add population number to each kreis)
merged_kreise: List[Kreis] = []
for kreis in kreise:
    normal_kreis_name: str = kreis.kreis.split("_")[0]

    # Find the kreis of kreis.json that corresponds to the kreis in the db
    match: KreisErweiterung
    if normal_kreis_name in kreis_and_town_both and "_Kreis" in kreis.kreis:
        match = next(filter(lambda new_info: new_info.name == normal_kreis_name
                                            and (new_info.type_of_kreis == "Kreis"
                                            or new_info.type_of_kreis == "Landkreis")
                                            and new_info.population_number != 0, kreis_typed))
    elif normal_kreis_name in kreis_and_town_both and not "_Kreis" in kreis.kreis:
        match = next(filter(lambda new_info: normal_kreis_name in new_info.name
                                            and (new_info.type_of_kreis == "Stadtkreis"
                                            or new_info.type_of_kreis == "Kreisfreie Stadt")
                                            and new_info.population_number != 0, kreis_typed))
    else:
        match = next(filter(lambda new_info: new_info.name == normal_kreis_name
                                             and new_info.population_number != 0, kreis_typed))
    new_kreis = Kreis(id=kreis.id, bundesland=kreis.bundesland, kreis=kreis.kreis, population=match.population_number)
    merged_kreise.append(new_kreis)
postgres_db.convert_to_db_entry(merged_kreise, "kreise").upsert()