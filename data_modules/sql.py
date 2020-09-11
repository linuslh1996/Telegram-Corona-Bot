from datetime import datetime
import data_modules.helper_functions as help

from psycopg2.sql import Composed, SQL, Identifier, Literal


def get_bundesland_cases_on_date(date: datetime.date, today: datetime.date) -> Composed:
    sql: SQL =  SQL("SELECT SUM(number_of_new_cases) AS new_cases, bundesland  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE f.date = {date} AND f.kreis_id IN (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                                "GROUP BY bundesland ORDER BY bundesland")
    return sql.format(date=Literal(date), today=Literal(today))

def get_kreiszahlen_of_bundesland(date: datetime.date, today: datetime.date, bundesland: str) -> Composed:
    sql: SQL = SQL("SELECT *  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE k.bundesland = {bundesland} AND f.date = {date} AND f.kreis_id IN "
                          "                 (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                          " ORDER BY k.kreis")
    return sql.format(today=Literal(today), date=Literal(date), bundesland=Literal(bundesland))


def get_history_for_kreis(kreis: str) -> Composed:
    sql: SQL = SQL("SELECT * FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
        "WHERE k.kreis = {kreis} ORDER BY f.date DESC")
    return sql.format(kreis=Literal(kreis))

def delete_all_from_before(date: datetime.date) -> Composed:
    print("Deleting Values")
    sql: SQL = SQL("DELETE FROM fallzahlen WHERE date <= {date}")
    return sql.format(date=Literal(date))



