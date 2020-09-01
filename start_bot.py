import sched
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as Time, timezone
from typing import Dict, List, Tuple

import os

import pytz
from psycopg2.sql import SQL, Literal
from telegram import Update
from data_modules import helper_functions as help, risklayer

from data_modules.database import PostgresDatabase, convert_to_type, convert_to_database_entry
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackContext
from data_modules.risklayer import KreisInformation
import threading

@dataclass
class BundeslandInfo:
    new_cases: int
    bundesland: str

@dataclass
class ChatInfo:
    chat_id: str
    is_active: bool


def post_summary(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    message_markdown: str = get_summarized_case_number(postgres_db)
    update.message.reply_markdown_v2(message_markdown)


def notify_user(context: CallbackContext, postgres_db: PostgresDatabase):
    message_markdown: str = get_summarized_case_number(postgres_db)
    context.bot.send_message(chat_id=context.job.context, text=message_markdown, parse_mode="MarkdownV2")

def get_summarized_case_number(postgres_db: PostgresDatabase) -> str:
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
        bundesland_name: str = help.escape_markdown_unsafe(help.replace_special_characters(info_today.bundesland))
        markdown += f'{emoji} */{bundesland_name}*: {info_today.new_cases} \({info_last_week.new_cases}\) \n'
    markdown = help.escape_markdown_safe(markdown)
    return markdown


def get_data_for_bundesland(update: Update, context: CallbackContext, postgres_db: PostgresDatabase, bundesland: str):
    # Define Query
    today: datetime.date = datetime.date(help.get_current_german_time())
    sql_kreis_cases = SQL("SELECT *  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE k.bundesland = {bundesland} AND f.date = {date} AND f.kreis_id IN "
                          "                 (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                          " ORDER BY k.kreis")
    # Get Results
    result: List[Dict] = postgres_db.get(sql_kreis_cases.format(date=Literal(today - timedelta(days=7)), bundesland=Literal(bundesland), today=Literal(today)))
    data_one_week_ago: List[KreisInformation] = convert_to_type(result, KreisInformation)
    result: List[Dict] = postgres_db.get(sql_kreis_cases.format(date=Literal(today), bundesland=Literal(bundesland), today=Literal(today)))
    data_today: List[KreisInformation] = convert_to_type(result, KreisInformation)
    # Construct Message
    combined_info: List[Tuple[KreisInformation, KreisInformation]] = list(zip(data_today, data_one_week_ago))
    markdown = f"*{bundesland}*:\n"
    for kreis in combined_info:
        emoji: str = get_emoji_for_case_numbers(kreis[1].number_of_new_cases, kreis[0].number_of_new_cases)
        kreis_name = help.escape_markdown_unsafe(help.replace_special_characters(kreis[0].kreis))
        markdown += f"{emoji} */{kreis_name}*: " \
                    f"{kreis[0].number_of_new_cases} \({kreis[1].number_of_new_cases}\) \n"
    update.message.reply_markdown_v2(help.escape_markdown_safe(markdown))


def get_data_for_kreis(update: Update, context: CallbackContext, postgres_db: PostgresDatabase, kreis: str):
    # Define Query
    sql_kreis_cases = SQL("SELECT * FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                          "WHERE k.kreis = {kreis} ORDER BY f.date DESC")
    # Get Results
    result: List[Dict] = postgres_db.get(sql_kreis_cases.format(kreis=Literal(kreis)))
    kreis_cases_history: List[KreisInformation] = convert_to_type(result, KreisInformation)
    # Construct Message
    markdown = f"*{help.escape_markdown_unsafe(kreis)}*:\n"
    markdown += "*Last Seven Days:* "
    for kreis_history in kreis_cases_history[1:8]:
        markdown += f"{kreis_history.number_of_new_cases}-"
    markdown = markdown[:-1]
    markdown += "\n"
    markdown += "*Average*: "
    markdown += f"{round(sum([kreis.number_of_new_cases for kreis in kreis_cases_history[1:8]])/7, 2)} \n"
    markdown += f"*Link:* [{help.escape_markdown_unsafe(kreis_cases_history[0].kreis)}]({help.escape_markdown_unsafe(kreis_cases_history[0].link)})"
    update.message.reply_markdown_v2(help.escape_markdown_safe(markdown))


def start_notifications(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    db_entry = {"chat_id": update.message.chat_id, "is_active": True}
    postgres_db.upsert("notifications", [db_entry])
    update.message.reply_text("You will now get a notification each day at 22h")


def stop_notifications(update: Update, context: CallbackContext):
    db_entry = {"chat_id": update.message.chat_id, "is_active": False}
    postgres_db.upsert("notifications", [db_entry])
    update.message.reply_text("You succesfully unsubscribed")


def get_emoji_for_case_numbers(cases_last_week: int, cases_this_week: int) -> str:
    if cases_this_week > 1.25 * cases_last_week:
        return "🛑"
    elif cases_this_week < 0.8 * cases_last_week or cases_last_week == 0:
        return "✅"
    else:
        return "⚠️"


def update_data_periodically(database: PostgresDatabase, api_key: str):
    scheduler = sched.scheduler(time.time, time.sleep)
    seconds_to_wait: int = 600
    help.periodic(scheduler, seconds_to_wait, lambda: update_data(database, api_key))
    scheduler.run()


def update_data(database: PostgresDatabase, api_key: str):
    print("Updating Values")
    kreis_infos: List[KreisInformation] = risklayer.get_new_data(api_key)
    data_was_resetted: bool = help.get_current_german_time().hour > 18 and sum([kreis.number_of_new_cases for kreis in kreis_infos]) < 100
    if data_was_resetted:
        return
    as_table_format: List[Dict] = convert_to_database_entry(kreis_infos, "fallzahlen", database)
    database.upsert("fallzahlen", as_table_format)


def get_users_to_notifiy(postgres_db: PostgresDatabase) -> List[str]:
    sql: SQL = SQL("SELECT * FROM notifications WHERE is_active = True")
    chat_info: List[ChatInfo] = convert_to_type(postgres_db.get(sql), ChatInfo)
    return [info.chat_id for info in chat_info]


# Load Data
API_KEY: str = os.environ["API_KEY"]
TELEGRAM_TOKEN: str = os.environ["TELEGRAM_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
postgres_db: PostgresDatabase = PostgresDatabase(DATABASE_URL)
# Schedule Updates
update_database_thread = threading.Thread(target=lambda: update_data_periodically(postgres_db, API_KEY))
update_database_thread.start()
# Schedule Notifications
updater: Updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
for user in get_users_to_notifiy(postgres_db):
    berlin = pytz.timezone('Europe/Berlin')
    time_where_notifications_get_send: Time = Time(hour=20, tzinfo=timezone.utc)
    updater.job_queue.run_daily(lambda context: notify_user(context, postgres_db),time_where_notifications_get_send, context=user)
# Register Functions To Dispatcher
dispatcher: Dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler("update", lambda update, context: post_summary(update, context, postgres_db)))
dispatcher.add_handler(CommandHandler("start", lambda update, context: start_notifications(update, context, postgres_db)))
dispatcher.add_handler(CommandHandler("stop", stop_notifications))
for bundesland in risklayer.get_all_bundeslaender(postgres_db):
    name_without_special_characters: str = help.replace_special_characters(bundesland)
    callback_function = lambda update, context, bundesland=bundesland: \
        get_data_for_bundesland(update, context, postgres_db, bundesland)
    dispatcher.add_handler(CommandHandler(name_without_special_characters, callback_function))
for kreis in risklayer.get_all_kreise(postgres_db):
    name_without_special_characters: str = help.replace_special_characters(kreis)
    callback_function = lambda update, context, kreis=kreis: \
        get_data_for_kreis(update, context, postgres_db, kreis)
    dispatcher.add_handler(CommandHandler(name_without_special_characters, callback_function))

updater.start_polling()









