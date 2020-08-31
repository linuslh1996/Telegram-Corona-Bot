from datetime import datetime

import pytz


def periodic(scheduler, interval, action, actionargs=()):
    scheduler.enter(interval, 1, periodic,
                    (scheduler, interval, action, actionargs))
    action(*actionargs)

def get_current_german_time() -> datetime:
    tz = pytz.timezone('Europe/Berlin')
    berlin_now: datetime = datetime.now(tz)
    return berlin_now

def escape_markdown_unsafe(unescaped_markdown: str) -> str:
    escaped_unsafe: str = unescaped_markdown.replace("(", "\(")
    escaped_unsafe = escaped_unsafe.replace(")", "\)")
    escaped_unsafe = escaped_unsafe.replace("*", "\*")
    escaped_unsafe = escaped_unsafe.replace("_", "\_")
    escaped_unsafe = escaped_unsafe.replace("%", "\%")
    return escaped_unsafe

def escape_markdown_safe(unescaped_markdown: str) -> str:
    markdown = unescaped_markdown.replace("-", "\-")
    markdown = markdown.replace(".", "\.")
    markdown = markdown.replace("+", "\+")
    markdown = markdown.replace("#", "\#")
    markdown = markdown.replace("=", "\=")
    return markdown