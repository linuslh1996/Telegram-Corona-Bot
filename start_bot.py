import sched
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, time as Time, timezone
from typing import List, Tuple

import os

from psycopg2.sql import SQL, Literal
from telegram import Update
from data_modules import helper_functions as help, risklayer

from data_modules.database import PostgresDatabase
from telegram.ext import Updater, Dispatcher, CommandHandler, CallbackContext
from data_modules.risklayer import KreisInformation
import threading

from data_modules.scheme import get_table_metadata


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

    data_one_week_ago: List[BundeslandInfo] = postgres_db\
        .get(sql_bundesland_cases.format(date=Literal(today - timedelta(days=7)), today=Literal(today)))\
        .convert_rows_to(BundeslandInfo)
    data_today: List[BundeslandInfo] = postgres_db\
        .get(sql_bundesland_cases.format(date=Literal(today), today=Literal(today)))\
        .convert_rows_to(BundeslandInfo)
    # Combine
    combined_info: List[Tuple[BundeslandInfo, BundeslandInfo]] = list(zip(data_today, data_one_week_ago))
    sorted_desc_by_growth: List[Tuple[BundeslandInfo, BundeslandInfo]] = sorted(combined_info, key=lambda info: info[0].new_cases - info[1].new_cases, reverse=True)
    # Construct And Send Message
    markdown: str = f"Today there are *{sum([data.new_cases for data in data_today])}* new cases so far. For the same districts, there" \
                    f" were *{sum([data.new_cases for data in data_one_week_ago])}* cases last week.\n \n"
    for bundesland in sorted_desc_by_growth:
        info_today: BundeslandInfo = bundesland[0]
        info_last_week: BundeslandInfo = bundesland[1]
        emoji: str = get_emoji_for_case_numbers(int(info_last_week.new_cases), int(info_today.new_cases))
        bundesland_name: str = help.escape_markdown_chars(create_bundesland_command(info_today.bundesland))
        markdown += f'{emoji} */{bundesland_name}*: {info_today.new_cases} \({info_last_week.new_cases}\) \n'
    markdown = help.escape_unnormal_markdown_chars(markdown)
    return markdown


def get_data_for_bundesland(update: Update, context: CallbackContext, postgres_db: PostgresDatabase, bundesland: str):
    # Define Query
    today: datetime.date = datetime.date(help.get_current_german_time())
    sql_kreis_cases = SQL("SELECT *  FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                                 "WHERE k.bundesland = {bundesland} AND f.date = {date} AND f.kreis_id IN "
                          "                 (SELECT kreis_id FROM fallzahlen WHERE date = {today} AND is_already_entered = True)"
                          " ORDER BY k.kreis")
    # Get Results
    data_one_week_ago: List[KreisInformation] = postgres_db\
        .get(sql_kreis_cases.format(date=Literal(today - timedelta(days=7)), bundesland=Literal(bundesland), today=Literal(today)))\
        .convert_rows_to(KreisInformation)
    data_today: List[KreisInformation] = postgres_db\
        .get(sql_kreis_cases.format(date=Literal(today), bundesland=Literal(bundesland), today=Literal(today)))\
        .convert_rows_to(KreisInformation)
    # Construct Message
    combined_info: List[Tuple[KreisInformation, KreisInformation]] = list(zip(data_today, data_one_week_ago))
    markdown = f"*{bundesland}*:\n"
    for kreis in combined_info:
        emoji: str = get_emoji_for_case_numbers(kreis[1].number_of_new_cases, kreis[0].number_of_new_cases)
        kreis_name = help.escape_markdown_chars(create_kreis_command(kreis[0].kreis))
        markdown += f"{emoji} */{kreis_name}*: " \
                    f"{kreis[0].number_of_new_cases} \({kreis[1].number_of_new_cases}\) \n"
    update.message.reply_markdown_v2(help.escape_unnormal_markdown_chars(markdown))


def get_data_for_kreis(update: Update, context: CallbackContext, postgres_db: PostgresDatabase, kreis: str):
    # Define Query
    sql_kreis_cases = SQL("SELECT * FROM fallzahlen f LEFT JOIN kreise k ON f.kreis_id = k.id "
                          "WHERE k.kreis = {kreis} ORDER BY f.date DESC")
    # Get Results
    kreis_cases_history: List[KreisInformation] = postgres_db\
        .get(sql_kreis_cases.format(kreis=Literal(kreis)))\
        .convert_rows_to(KreisInformation)
    # Construct Message
    markdown = f"*{help.escape_markdown_chars(kreis)}*:\n"
    markdown += "*Last Seven Days:* "
    for kreis_history in kreis_cases_history[1:8]:
        markdown += f"{kreis_history.number_of_new_cases}-"
    markdown = markdown[:-1]
    markdown += "\n"
    markdown += "*Average*: "
    markdown += f"{round(sum([kreis.number_of_new_cases for kreis in kreis_cases_history[1:8]])/7, 2)} \n"
    markdown += f"*Link:* [{help.escape_markdown_chars(kreis_cases_history[0].kreis)}]({help.escape_markdown_chars(kreis_cases_history[0].link)})"
    update.message.reply_markdown_v2(help.escape_unnormal_markdown_chars(markdown))


def start_notifications(update: Update, context: CallbackContext, postgres_db: PostgresDatabase):
    db_entry = {"chat_id": update.message.chat_id, "is_active": True}
    postgres_db.upsert("notifications", [db_entry])
    update.message.reply_text("You will now get a notification each day at 22h!")


def stop_notifications(update: Update, context: CallbackContext):
    db_entry = {"chat_id": update.message.chat_id, "is_active": False}
    postgres_db.upsert("notifications", [db_entry])
    update.message.reply_text("You succesfully unsubscribed!")


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
    print(f"Updating Values, date={help.get_current_german_time()}")
    kreis_infos: List[KreisInformation] = risklayer.get_new_data(api_key)
    print(kreis_infos)
    data_was_resetted: bool = help.get_current_german_time().hour > 18 and sum([kreis.number_of_new_cases for kreis in kreis_infos]) < 100
    if data_was_resetted:
        print("data was resetted")
        return
    database.convert_to_db_entry(kreis_infos, "fallzahlen").upsert()


def get_users_to_notifiy(postgres_db: PostgresDatabase) -> List[str]:
    sql: SQL = SQL("SELECT * FROM notifications WHERE is_active = True")
    chat_info: List[ChatInfo] = postgres_db.get(sql).convert_rows_to(ChatInfo)
    return [info.chat_id for info in chat_info]

def create_bundesland_command(bundesland_unformatted: str) -> str:
    without_special_characters: str = help.normalise_string(bundesland_unformatted)
    return without_special_characters

def create_kreis_command(kreis_unformatted: str) -> str:
    without_special_characters: str = help.normalise_string(kreis_unformatted)
    if without_special_characters in ["Bremen", "Hamburg", "Berlin"]:
        without_special_characters += "_K"
    return without_special_characters




# Load Data
API_KEY: str = os.environ["API_KEY"]
TELEGRAM_TOKEN: str = os.environ["TELEGRAM_TOKEN"]
DATABASE_URL: str = os.environ["DATABASE_URL"]
postgres_db: PostgresDatabase = PostgresDatabase(DATABASE_URL)
postgres_db.initialize_tables(DATABASE_URL, get_table_metadata())
# Schedule Updates
update_database_thread = threading.Thread(target=lambda: update_data_periodically(postgres_db, API_KEY))
update_database_thread.start()
# Schedule Notifications
updater: Updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
for user in get_users_to_notifiy(postgres_db):
    time_where_notifications_get_send: Time = Time(hour=20, tzinfo=timezone.utc)
    updater.job_queue.run_daily(lambda context: notify_user(context, postgres_db),time_where_notifications_get_send, context=user)
# Register Functions To Dispatcher
dispatcher: Dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler("update", lambda update, context: post_summary(update, context, postgres_db)))
dispatcher.add_handler(CommandHandler("start", lambda update, context: start_notifications(update, context, postgres_db)))
dispatcher.add_handler(CommandHandler("stop", stop_notifications))
for bundesland in risklayer.get_all_bundeslaender(postgres_db):
    bundesland_command: str = create_bundesland_command(bundesland)
    callback_function = lambda update, context, bundesland=bundesland: \
        get_data_for_bundesland(update, context, postgres_db, bundesland)
    dispatcher.add_handler(CommandHandler(bundesland_command, callback_function))
for kreis in risklayer.get_all_kreise(postgres_db):
    kreis_command: str = create_kreis_command(kreis)
    callback_function = lambda update, context, kreis=kreis: \
        get_data_for_kreis(update, context, postgres_db, kreis)
    dispatcher.add_handler(CommandHandler(kreis_command, callback_function))

updater.start_polling()









