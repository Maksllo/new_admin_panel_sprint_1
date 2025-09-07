import sqlite3
import psycopg
from psycopg.rows import dict_row
import uuid
from datetime import datetime
import os
from dotenv import load_dotenv
from psycopg import ClientCursor, connection as _connection
from contextlib import closing
from dataclasses import dataclass, astuple
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row
from dotenv import load_dotenv
from datetime import datetime
from contextlib import closing
from dataclasses import dataclass, astuple
from typing import Generator
from uuid import UUID
from datetime import timezone

load_dotenv()
DB_SQLITE_PATH = os.environ.get('SQLITE_DB_PATH', 
    os.path.join(os.path.dirname(__file__), '..', '..', 'sqlite_to_postgres', 'db.sqlite'))
DB_SQLITE = 'db.sqlite'
BATCH_SIZE = 100


@dataclass
class FilmWork:
    id: UUID
    title: str
    description: str
    creation_date: datetime 
    file_path: str
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self):
        if isinstance(self.id, str):
         self.id = UUID(self.id)
        if self.description is None:
            self.description = ''
        elif not isinstance(self.description, str):
            self.description = str(self.description)
        if self.file_path is None:
            self.file_path = ''
        elif not isinstance(self.file_path, str):
            self.file_path = str(self.file_path)
        if self.type is None:
            self.type = ''
        elif not isinstance(self.type, str):
            self.type = str(self.type)
        if isinstance(self.created_at, str):
         self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
         self.updated_at = datetime.fromisoformat(self.updated_at)

@dataclass
class Person:
    id: UUID
    full_name: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self):
        if isinstance(self.id, str):
         self.id = UUID(self.id)
        if isinstance(self.created_at, str):
         self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
         self.updated_at = datetime.fromisoformat(self.updated_at)

@dataclass
class Genre:
    id: UUID
    name: str
    description: str
    created_at: datetime
    updated_at: datetime

    def __post_init__(self):
        if isinstance(self.id, str):
         self.id = UUID(self.id)
        if self.description is None:
            self.description = ''
        elif not isinstance(self.description, str):
            self.description = str(self.description)
        if isinstance(self.created_at, str):
         self.created_at = datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
         self.updated_at = datetime.fromisoformat(self.updated_at)

@dataclass
class GenreFilmWork:
    id: UUID
    film_work_id: UUID
    genre_id: UUID
    created_at: datetime

    def __post_init__(self):
        if isinstance(self.id, str):
         self.id = UUID(self.id)
        if isinstance(self.genre_id, str):
         self.genre_id = UUID(self.genre_id)
        if isinstance(self.film_work_id, str):
         self.film_work_id = UUID(self.film_work_id)
        if isinstance(self.created_at, str):
         self.created_at = datetime.fromisoformat(self.created_at)

@dataclass
class PersonFilmWork:
    id: UUID
    film_work_id: UUID
    person_id: UUID
    role: str
    created_at: datetime

    def __post_init__(self):
        if isinstance(self.id, str):
         self.id = UUID(self.id)
        if isinstance(self.film_work_id, str):
         self.film_work_id = UUID(self.film_work_id)  
        if isinstance(self.person_id, str):
         self.person_id = UUID(self.person_id)
        if isinstance(self.created_at, str):
         self.created_at = datetime.fromisoformat(self.created_at)

TABLES = {'genre': (Genre), 'film_work': FilmWork, 'person': Person, 'genre_film_work': GenreFilmWork, 'person_film_work': PersonFilmWork}

def deduplicate_objects( objects, table_name: str) -> list:
    seen = set()
    result = []
    for obj in objects:
        if table_name == 'person_film_work':
            key = (obj.film_work_id, obj.person_id)
        elif table_name == 'genre_film_work':
            key = (obj.film_work_id, obj.genre_id)
        else:
            key = obj.id
        if key not in seen:
            seen.add(key)
            result.append(obj)
    return result

def test_transfer(sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg.Cursor):
    for table_name,(dataclass_type) in TABLES.items():
        print(f'Тестируем таблицу {table_name}')
        sqlite_cursor.execute('SELECT * FROM {table_name}'.format(table_name=table_name))
        all_sqlite_rows = []
        while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
            all_sqlite_rows.extend(batch)
        original_batch = [dataclass_type(**dict(table_name)) for table_name in all_sqlite_rows]

        if table_name in ('person_film_work', 'genre_film_work'):
            original_batch = deduplicate_objects(original_batch, table_name)

        ids = [obj.id for obj in original_batch]

        pg_cursor.execute(f'SELECT * FROM content.{table_name} WHERE id = ANY(%s)', [ids])
        pg_rows = pg_cursor.fetchall()

        transferred_batch = []
        for row in pg_rows:
            row_dict = dict(row)
            if 'created' in row_dict:
                row_dict['created_at'] = row_dict.pop('created')
            if 'modified' in row_dict:
                row_dict['updated_at'] = row_dict.pop('modified')
            transferred_batch.append(dataclass_type(**row_dict))

        original_batch.sort(key=lambda x: x.id)
        transferred_batch.sort(key=lambda x: x.id)
        assert len(original_batch) == len(transferred_batch)
        assert original_batch == transferred_batch 
        print(f"✅ Таблица {table_name} — OK")

if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST', '127.0.0.1'), 'port': os.environ.get('DB_PORT', 5432)}
    with closing(sqlite3.connect('/home/maksllo/new_admin_panel_sprint_1/sqlite_to_postgres/db.sqlite')) as sqlite_conn, closing(psycopg.connect(**dsl)) as pg_conn:
        sqlite_conn.row_factory = sqlite3.Row
        with closing(sqlite_conn.cursor()) as sqlite_cur, closing(pg_conn.cursor(row_factory=dict_row)) as pg_cur:
            test_transfer(sqlite_cur, pg_cur)

