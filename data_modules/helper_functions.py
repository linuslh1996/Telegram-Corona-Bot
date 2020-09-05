
from datetime import datetime
import logging

import pytz

def periodic(scheduler, interval, action, actionargs=()):
    scheduler.enter(interval, 1, periodic,
                    (scheduler, interval, action, actionargs))
    try:
        action(*actionargs)
    except Exception:
        logging.exception("Periodic Event Failed:")


def get_current_german_time() -> datetime:
    tz = pytz.timezone('Europe/Berlin')
    berlin_now: datetime = datetime.now(tz)
    return berlin_now

def escape_markdown_chars(unescaped_markdown: str) -> str:
    escaped_unsafe: str = unescaped_markdown.replace("(", "\(")
    escaped_unsafe = escaped_unsafe.replace(")", "\)")
    escaped_unsafe = escaped_unsafe.replace("*", "\*")
    escaped_unsafe = escaped_unsafe.replace("_", "\_")
    escaped_unsafe = escaped_unsafe.replace("%", "\%")
    return escaped_unsafe

def escape_unnormal_markdown_chars(unescaped_markdown: str) -> str:
    markdown = unescaped_markdown.replace("-", "\-")
    markdown = markdown.replace(".", "\.")
    markdown = markdown.replace("+", "\+")
    markdown = markdown.replace("#", "\#")
    markdown = markdown.replace("=", "\=")
    return markdown

# Needed because Telegram can not handle some characters in its commands
def normalise_string(normal_german_string: str) -> str:
    without_special_characters: str = normal_german_string.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    without_empty_spaces: str = without_special_characters.replace(" ", "_").replace("-", "_")
    without_other_characters = without_empty_spaces.replace("(", "").replace(")", "").replace(".", "")
    shortened: str = without_other_characters[:25]
    return shortened