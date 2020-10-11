from sqlalchemy import Column, Integer, ForeignKey, String, Date, Boolean, MetaData
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

Base = declarative_base()


class Kreis(Base):
    __tablename__ = "kreise"

    id = Column(Integer, primary_key=True)
    bundesland = Column(String)
    kreis = Column(String)
    population = Column(Integer)

class Fallzahl(Base):
    __tablename__ = "fallzahlen"

    kreis_id = Column(ForeignKey("kreise.id"), primary_key=True)
    date = Column(Date, primary_key=True)
    number_of_new_cases = Column(Integer)
    link = Column(String)
    is_already_entered = Column(Boolean)

class Notification(Base):
    __tablename__ = "notifications"

    chat_id = Column(Integer, primary_key=True)
    is_active = Column(Boolean)

def get_table_metadata() -> MetaData:
    return Base.metadata



