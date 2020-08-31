from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import sys

from psycopg2.sql import SQL, Literal
from telegram import Update

from database import PostgresDatabase, convert_to_type
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackContext
import risklayer

@dataclass
class BundeslandInfo:
    new_cases: int
    bundesland: str

def update_case_number(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    # Define Query
    today: datetime.date = datetime.date(datetime.now())
    sql_bundesland_cases = SQL("SELECT SUM(number_of_new_cases) AS new_cases, bundesland  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE f.date = {date} AND f.kreis_id IN (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                                "GROUP BY bundesland")
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
        markdown += f'*{info_today.bundesland}*: {info_today.new_cases} ({info_last_week.new_cases}) \n'
    markdown = escape_markdown(markdown)
    update.message.reply_markdown_v2(markdown)


def escape_markdown(unescaped_markdown: str) -> str:
    markdown = unescaped_markdown.replace("-", "\-")
    markdown = markdown.replace(".", "\.")
    markdown = markdown.replace("+", "\+")
    markdown = markdown.replace("(", "\(")
    markdown = markdown.replace(")", "\)")
    return markdown

API_KEY: str = sys.argv[1]
TELEGRAM_TOKEN: str = sys.argv[2]

postgres_db: PostgresDatabase = PostgresDatabase("postgres", "admin", "localhost", "5432", "coronabot")

updater: Updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
dispatcher: Dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler("update", lambda update, context: update_case_number(update, context, postgres_db)))
updater.start_polling()









