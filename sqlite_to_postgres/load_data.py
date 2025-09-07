import sqlite3
import psycopg
import os
import logging
from psycopg import ClientCursor, connection as _connection
from psycopg.rows import dict_row
from dotenv import load_dotenv
from datetime import datetime
from contextlib import closing
from dataclasses import dataclass, astuple
from typing import Generator
from uuid import UUID


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

load_dotenv()

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


class SQLiteLoader:
    def __init__(self, conn):
        self.conn = conn
        self.BATCH_SIZE = 100  
        self.TABLE_CONFIG = {
        'person': ('id, full_name, created_at, updated_at', Person),
        'genre': ('id, name, description, created_at, updated_at', Genre),
        'film_work': ('id, title, description, creation_date, file_path, rating, type, created_at, updated_at', FilmWork),
        'genre_film_work': ('id, film_work_id, genre_id, created_at', GenreFilmWork),
        'person_film_work': ('id, film_work_id, person_id, role, created_at', PersonFilmWork)
        }

    def extract_table(self,table_name: str, columns: str) -> Generator[list[sqlite3.Row], None, None]:
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        cursor.execute(f'SELECT {columns} FROM {table_name};')
        while batch := cursor.fetchmany(self.BATCH_SIZE):
            yield batch

    def transform_table(self, table_name: str, columns: str, dataclass_type) -> Generator[list, None, None]:
        for batch in self.extract_table(table_name, columns):
            items = []
            for row in batch:
                try:
                    item = dataclass_type(**dict(row))
                    items.append(item)
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка при создании {dataclass_type.__name__} из строки {dict(row)}: {e}")
                    continue
            yield items

    def load_all_data(self) -> dict[str, list]:
        data = {}
        for table_name, (columns, dataclass_type) in self.TABLE_CONFIG.items():
            logger.info(f"Загрузка таблицы {table_name}...")
            data[table_name] = []
            for batch in self.transform_table(table_name, columns, dataclass_type):
                data[table_name].extend(batch)
            logger.info(f"✅ Загружено {len(data[table_name])} записей из {table_name}")
        return data


class PostgresSaver:
    def __init__(self, connection: _connection):
        self.connection = connection
        self.TABLE_CONFIG = {
            'film_work': 'content.film_work',
            'genre': 'content.genre',
            'person': 'content.person',
            'genre_film_work': 'content.genre_film_work',
            'person_film_work': 'content.person_film_work',
        }

    def save_all_data(self, data: dict):
        for table_name, records in data.items():
            if not records:
                continue

            if table_name not in self.TABLE_CONFIG:
                logger.warning(f"Неизвестная таблица: {table_name}")
                continue

            table_sql_name = self.TABLE_CONFIG[table_name]
            self._save_table(table_sql_name, records)
            logger.info(f"✅ Сохранено {len(records)} записей в {table_sql_name}")

    def _save_table(self, table_name: str, records: list):
        if not records:
            return
        FIELD_MAPPING = {
            'content.person': {'created_at': 'created', 'updated_at': 'modified'},
            'content.film_work': {'created_at': 'created', 'updated_at': 'modified'},
            'content.genre': {'created_at': 'created', 'updated_at': 'modified'},
            'content.genre_film_work': {'created_at': 'created'},
            'content.person_film_work': {'created_at': 'created'},
        }

        ON_CONFLICT_FIELDS = {
        'content.person_film_work': '(film_work_id, person_id)',
        }

        conflict_fields = ON_CONFLICT_FIELDS.get(table_name, '(id)')
        field_mapping = FIELD_MAPPING.get(table_name, {})
        sample_record = records[0]
        original_fields = list(sample_record.__dict__.keys())
        db_fields = [field_mapping.get(f, f) for f in original_fields]
        fields_sql = ', '.join(db_fields)
        params_sql = ', '.join([f'%({f})s' for f in db_fields]) 

        insert_query = f"""
            INSERT INTO {table_name} ({fields_sql})
            VALUES ({params_sql})
            ON CONFLICT {conflict_fields} DO NOTHING;
        """

        with self.connection.cursor() as cur:
            for record in records:
                try:
                    params = {}
                    for orig_field, db_field in zip(original_fields, db_fields):
                        value = getattr(record, orig_field)
                        params[db_field] = value

                    cur.execute(insert_query, params)
                except Exception as e:
                    logger.error(f"Ошибка при вставке записи {record} в {table_name}: {e}")
                    raise
            self.connection.commit()

def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    data = sqlite_loader.load_all_data()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST', '127.0.0.1'), 'port': os.environ.get('DB_PORT', 5432)}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg.connect(
        **dsl, row_factory=dict_row, cursor_factory=ClientCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
