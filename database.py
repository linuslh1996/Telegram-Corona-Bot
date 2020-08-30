from typing import List, Dict, Any, Tuple

import psycopg2
from psycopg2.extensions import cursor as Cursor
from psycopg2.extensions import connection as Connection

from psycopg2.sql import SQL, Identifier, Composable, Literal
from psycopg2.extras import execute_values


class PostgresDatabase:

    def __init__(self, username: str, password: str, host: str, port: str, database: str):
        self.connection: Connection = psycopg2.connect(user=username,
                                                       password=password,
                                                       host=host,
                                                       port=port,
                                                       database=database)
        self.cursor: Cursor = self.connection.cursor()

    def get_table_names(self) -> List[str]:
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public'""")
        tables: List[str] = self.cursor.fetchall()
        return tables

    def get_column_names(self, table_name: str) -> List[str]:
        self.cursor.execute(SQL("SELECT * FROM {}").format(Identifier(table_name)))
        columnnames: List[str] = [desc[0] for desc in self.cursor.description]
        return columnnames

    def insert(self, table_name: str, data: List[Dict[str, Any]]):
        keys: List[str] = list(data[0].keys())
        as_identifiers: List[Identifier] = [Identifier(key) for key in keys]
        values = [list(entry.values()) for entry in data]
        execute_values(self.cursor, SQL("INSERT INTO {table_name} ({fields}) VALUES %s") \
                       .format(table_name=Identifier(table_name), fields=SQL(",").join(as_identifiers)),
                       values)
        self.connection.commit()

    def upsert(self, table_name: str, data: List[Dict]):
        primary_keys: List[str] = self._get_primary_keys(table_name)
        column_names: List[Identifier] = [Identifier(key) for key in data[0].keys()]
        all_values: List[List[Any]] = [list(entry.values()) for entry in data]
        for values in all_values:
            remaining_columns: List[Identifier] = [column_name for column_name in column_names if
                                                   not column_name.string in primary_keys]
            as_literals: List[Literal] = [Literal(value) for value in values]
            primary_keys_as_identifier = [Identifier(key) for key in primary_keys]

            sql: Composable = SQL("INSERT INTO {table_name} ({fields}) VALUES ({as_literals}) "
                                  "ON CONFLICT ({primary_keys}) "
                                  "DO UPDATE SET ({remaining_columns}) = ({remaining_values})") \
                .format(table_name=Identifier(table_name), fields=SQL(",").join(column_names), as_literals=SQL(",").join(as_literals), primary_keys=SQL(",").join(primary_keys_as_identifier),
                        remaining_columns=SQL(",").join(remaining_columns), remaining_values=SQL("EXCLUDED.") + SQL(", EXCLUDED.").join(remaining_columns))
            self.cursor.execute(sql)
        self.connection.commit()

    def get(self, sql: Composable) -> List[Dict]:
        self.cursor.execute(sql)
        results_raw: List[Tuple] = self.cursor.fetchall()
        columnnames: List[str] = [desc[0] for desc in self.cursor.description]
        results: List[Dict] = []
        for result in results_raw:
            entry: Dict = dict(zip(columnnames, result))
            results.append(entry)
        return results

    def _get_primary_keys(self, table_name: str) -> List[str]:
        sql: Composable = SQL("SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type "
                              "FROM pg_index i JOIN  pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                              "WHERE  i.indrelid = {table_name}::regclass AND i.indisprimary") \
            .format(table_name=Literal(table_name))
        results: List[Dict] = self.get(sql)
        return [result["attname"] for result in results]
