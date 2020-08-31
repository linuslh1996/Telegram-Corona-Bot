import sched
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import sys
import os


from psycopg2.sql import SQL, Literal
from telegram import Update
import helper_functions as help

from database import PostgresDatabase, convert_to_type, convert_to_database_entry
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackContext
import risklayer
from risklayer import KreisInformation
from sched import scheduler
import threading

@dataclass
class BundeslandInfo:
    new_cases: int
    bundesland: str


def get_summarized_case_number(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    # Define Query
    today: datetime.date = datetime.date(help.get_current_german_time())
    sql_bundesland_cases = SQL("SELECT SUM(number_of_new_cases) AS new_cases, bundesland  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE f.date = {date} AND f.kreis_id IN (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                                "GROUP BY bundesland ORDER BY bundesland")
    # Get Results
    result: List[Dict] = postgres_db.get(sql_bundesland_cases.format(date=Literal(today - timedelta(days=7)), today=Literal(today)))
    data_one_week_ago: List[BundeslandInfo] = convert_to_type(result, BundeslandInfo)
    result: List[Dict] = postgres_db.get(sql_bundesland_cases.format(date=Literal(today), today=Literal(today)))
    data_today: List[BundeslandInfo] = convert_to_type(result, BundeslandInfo)
    # Combine
    combined_info: List[Tuple[BundeslandInfo, BundeslandInfo]] = list(zip(data_today, data_one_week_ago))
    sorted_descending: List[Tuple[BundeslandInfo, BundeslandInfo]] = sorted(combined_info, key=lambda info: info[0].new_cases - info[1].new_cases, reverse=True)
    # Construct And Send Message
    markdown: str = f"Today there are *{sum([data.new_cases for data in data_today])}* new cases so far. For the same districts, there" \
                    f" were *{sum([data.new_cases for data in data_one_week_ago])}* cases last week.\n \n"
    for bundesland in sorted_descending:
        info_today: BundeslandInfo = bundesland[0]
        info_last_week: BundeslandInfo = bundesland[1]
        emoji: str = get_emoji_for_case_numbers(int(info_last_week.new_cases), int(info_today.new_cases))
        markdown += f'{emoji}*{info_today.bundesland}*: {info_today.new_cases} \({info_last_week.new_cases}\) \n'
    markdown = help.escape_markdown_safe(markdown)
    update.message.reply_markdown_v2(markdown)


def get_data_for_all_kreise(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    # Define Query
    today: datetime.date = datetime.date(datetime.now())
    sql_kreis_cases = SQL("SELECT *  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE f.date = {date} AND f.kreis_id IN (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                          " ORDER BY k.bundesland, k.kreis")
    # Get Results
    result: List[Dict] = postgres_db.get(sql_kreis_cases.format(date=Literal(today - timedelta(days=7)), today=Literal(today)))
    data_one_week_ago: List[KreisInformation] = convert_to_type(result, KreisInformation)
    result: List[Dict] = postgres_db.get(sql_kreis_cases.format(date=Literal(today), today=Literal(today)))
    data_today: List[KreisInformation] = convert_to_type(result, KreisInformation)
    # Construct Message
    combined_info: List[Tuple[KreisInformation, KreisInformation]] = list(zip(data_today, data_one_week_ago))
    markdown = ""
    current_bundesland = ""
    for kreis in combined_info:
        if current_bundesland != kreis[0].bundesland:
            markdown = help.escape_markdown_safe(markdown)
            if markdown != "":
                update.message.reply_markdown_v2(markdown)
            current_bundesland = kreis[0].bundesland
            markdown = f"*{current_bundesland}*:\n"
        emoji: str = get_emoji_for_case_numbers(kreis[1].number_of_new_cases, kreis[0].number_of_new_cases)
        markdown += f"{emoji}[{help.escape_markdown_unsafe(kreis[0].kreis)}](test): " \
                    f"{kreis[0].number_of_new_cases} \({kreis[1].number_of_new_cases}\) \n"
    update.message.reply_markdown_v2(help.escape_markdown_safe(markdown))

def get_emoji_for_case_numbers(cases_last_week: int, cases_this_week: int) -> str:
    if cases_this_week > 1.25 * cases_last_week:
        return "🛑"
    elif cases_this_week < 0.8 * cases_last_week or cases_last_week == 0:
        return "✅"
    else:
        return "⚠️"

def update_data(database: PostgresDatabase, api_key: str):
    print("Updating Values")
    kreis_infos: List[KreisInformation] = risklayer.get_new_data(api_key)
    data_was_resetted: bool = help.get_current_german_time().hour > 18 and sum([kreis.number_of_new_cases for kreis in kreis_infos]) < 100
    if data_was_resetted:
        return
    as_table_format: List[Dict] = convert_to_database_entry(kreis_infos, "fallzahlen", database)
    database.upsert("fallzahlen", as_table_format)

def update_data_periodically(database: PostgresDatabase, api_key: str):
    scheduler = sched.scheduler(time.time, time.sleep)
    seconds_to_wait: int = 600
    help.periodic(scheduler, seconds_to_wait, lambda: update_data(database, api_key))
    scheduler.run()


API_KEY: str = os.environ["API_KEY"]
TELEGRAM_TOKEN: str = os.environ["TELEGRAM_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
postgres_db: PostgresDatabase = PostgresDatabase(DATABASE_URL)

update_database_thread = threading.Thread(target=lambda: update_data_periodically(postgres_db, API_KEY))
update_database_thread.start()

updater: Updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
dispatcher: Dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler("update", lambda update, context: get_summarized_case_number(update, context, postgres_db)))
dispatcher.add_handler(CommandHandler("all", lambda update, context: get_data_for_all_kreise(update, context, postgres_db)))
updater.start_polling()









