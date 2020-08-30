# Relies heavily on the knowledge about how the risklayer table is set up. Basically the kreise are sorted by Bundesland
from typing import List


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