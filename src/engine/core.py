import sqlite3
from ..schema.model import Table, Field


def get_sql(table: Table) -> str:
    