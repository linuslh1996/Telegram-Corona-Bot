from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List

import sys

from psycopg2.sql import SQL, Literal
from telegram import Update

from database import PostgresDatabase, convert_to_type
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackContext


@dataclass
class BundeslandInfo:
    new_cases: int
    bundesland: str

def update_case_number(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    today: datetime.date = datetime.date(datetime.now()) - timedelta(days=1)
    result: List[Dict] = postgres_db.get(SQL("SELECT SUM(number_of_new_cases) AS new_cases, bundesland  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE f.date = {today} "
                                "GROUP BY bundesland").format(today=Literal(today)))
    bundesland_data: List[BundeslandInfo] = convert_to_type(result, BundeslandInfo)
    sorted_descending: List[BundeslandInfo] = sorted(bundesland_data, key=lambda bundesland: bundesland.new_cases, reverse=True)
    markdown: str = ""
    for bundesland in sorted_descending:
        markdown += f'*{bundesland.bundesland}*: {bundesland.new_cases} \n'
    markdown = markdown.replace("-", "\-")
    update.message.reply_markdown_v2(markdown)

postgres_db: PostgresDatabase = PostgresDatabase("postgres", "admin", "localhost", "5432", "coronabot")

API_KEY: str = sys.argv[1]
TELEGRAM_TOKEN: str = sys.argv[2]

updater: Updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
dispatcher: Dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler("update", lambda update, context: update_case_number(update, context, postgres_db)))
updater.start_polling()









