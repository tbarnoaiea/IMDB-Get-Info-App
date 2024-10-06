import requests
import logging
import pandas as pd
import sqlite3
from typing import Any

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)

endpoints = {'get_fan_favorites': 'getFanFavorites',
             'get_week_top_10': 'getWeekTop10',
             'get_whats_streaming': 'getWhatsStreaming'}  # a dict to map 3 endpoints


def generate_report_top_week_list():
    movies_list = []
    conn = sqlite3.connect('imdb.db')  # connect to DB
    cursor = conn.cursor()

    # Demonstrate DB relationships between the tables
    cursor.execute('''
        SELECT week_top.name
        FROM week_top
        JOIN movies ON week_top.movies_id = movies.movies_id;
    ''')

    report = cursor.fetchall()
    logger.info("Report generated: Top week movies")

    for row in report:
        movies_list.append(row[0])
    with open('report.txt', 'w') as file:
        file.write(f"Top week movies: {movies_list}")
    file.close()

    conn.close()


# Class for creating tables in DB
class Table:
    def __init__(self, table_name: str) -> None:
        self._conn = None
        self._table_name = table_name
        self._content = None
        self._column: tuple = ()

    @property
    def table_name(self):
        return self._table_name

    def create_table(self, **data) -> None:
        table = f'''CREATE TABLE IF NOT EXISTS {self._table_name} ('''
        columns = list()
        for key, value in data.items():
            columns.append(key)
            if key == list(data)[-1]:
                table += f'''\n{key} {value}'''
                table += '''\n)'''
            elif key == 'FOREIGN_KEY':
                table += f'''\nFOREIGN KEY {value}'''
            else:
                table += f'''\n{key} {value},'''

        self._column = tuple(columns)
        self._content = table
        self._conn = sqlite3.connect('imdb.db')  # Create a DB named imdb and connect to it
        cursor = self._conn.cursor()
        cursor.execute(self._content)
        self._conn.commit()
        logger.info(f'Table {self._table_name} created if not exist')
        cursor.close()


# Here is stored the credential for RapidAPI
class IMDBdata(Table):
    def __init__(self, table_name: str) -> None:
        super().__init__(table_name)
        self._endpoint: str = ''
        self._headers = {"x-rapidapi-key": "1b0b5cc9d5msh4e4c4a407fff387p16aed6jsn43af16ee97a3",
                         "x-rapidapi-host": "imdb188.p.rapidapi.com"}

    @property
    def endpoint(self) -> str:
        return self._endpoint

    @endpoint.setter
    def endpoint(self, endpoint: str) -> None:
        self._endpoint = endpoint

    def get_data(self) -> dict | str:
        endpoint_value = self._endpoint

        response = requests.get(f'https://imdb188.p.rapidapi.com/api/v1/{endpoint_value}', headers=self._headers)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Error: {response.status_code}")
            return 'Error response: {}'.format(response)


# Set a class for the Fan Favorites endpoint
class FavoritesFan(IMDBdata):
    def __init__(self) -> None:
        super().__init__('fan_favorites')
        self.create_table(id='TEXT PRIMARY KEY',
                          name='TEXT',
                          category='TEXT',
                          rank='INTEGER',
                          votes='INTEGERS',
                          rating='REAL',
                          trailer='TEXT')
        self.endpoint = endpoints.get('get_fan_favorites')

    # Normalization data
    def filter_data(self) -> list[dict[str, str | int | None | float | Any]]:
        data = self.get_data()
        list_filtered_data = list()
        for item in data['data']['list']:
            filtered_data = dict()

            filtered_data.update({'id': item['id']})

            filtered_data['name'] = item['originalTitleText']['text']

            if item['ratingsSummary']['topRanking'] is not None:
                filtered_data['rank'] = int(item['ratingsSummary']['topRanking']['rank'])
            else:
                filtered_data['rank'] = None

            filtered_data['votes'] = int(item['ratingsSummary']['voteCount'])

            if item['ratingsSummary']['aggregateRating'] is not None:
                filtered_data['rating'] = float(item['ratingsSummary']['aggregateRating'])
            else:
                filtered_data['rating'] = None

            filtered_data['category'] = item['titleType']['categories'][0]['value']

            latest_trailer = item['latestTrailer']
            if latest_trailer is not None:
                filtered_data['trailer'] = latest_trailer.get('createdDate')
            else:
                filtered_data['trailer'] = None
            list_filtered_data.append(filtered_data)

        return list_filtered_data

    def insert_data_into_db(self) -> None:
        movies_table = Movies()
        data = self.filter_data()
        if type(data) is str:
            return logging.error('Failed to filter the data')
        for item in data:
            self._conn = sqlite3.connect('imdb.db')
            movies_table.insert_movies(item['name'])
            cursor = self._conn.cursor()
            try:
                cursor.execute(f'''
                           INSERT INTO {self.table_name} (id, name, category, rank, votes, rating, trailer)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                       ''', (
                    item['id'],
                    item['name'],
                    item['category'],
                    item['rank'],
                    item['votes'],
                    item['rating'],
                    item['trailer']
                ))
            except sqlite3.IntegrityError:
                logging.warning(f"This item[{item}] is already in the database")
            self._conn.commit()
            self._conn.close()


# Set a class for the Week Top 10 endpoint
class WeekTop(IMDBdata):
    def __init__(self):
        self.__movies_ids = list()
        super().__init__('week_top')
        self.create_table(id='TEXT PRIMARY KEY',
                          name='TEXT',
                          category='TEXT',
                          rank='INTEGER',
                          votes='INTEGERS',
                          rating='REAL',
                          release='TEXT',
                          movies_id='INTEGER',
                          FOREIGN_KEY='movies_id REFERENCES movies(movies_id)'
                          )
        self.endpoint = endpoints.get('get_week_top_10')

    # Normalization data
    def filter_data(self) -> list[dict[str, str | int | None | float | Any]]:
        data = self.get_data()
        list_filtered_data = list()
        for item in data['data']:
            filtered_data = dict()

            filtered_data.update({'id': item['id']})

            filtered_data['name'] = item['originalTitleText']['text']

            if item['ratingsSummary']['topRanking'] is not None:
                filtered_data['rank'] = int(item['ratingsSummary']['topRanking']['rank'])
            else:
                filtered_data['rank'] = None

            filtered_data['votes'] = int(item['ratingsSummary']['voteCount'])

            if item['ratingsSummary']['aggregateRating'] is not None:
                filtered_data['rating'] = float(item['ratingsSummary']['aggregateRating'])
            else:
                filtered_data['rating'] = None

            filtered_data['category'] = item['titleType']['categories'][0]['value']
            filtered_data['release'] = item['releaseYear']['year']

            list_filtered_data.append(filtered_data)

        return list_filtered_data

    def insert_data_into_db(self) -> None:
        data = self.filter_data()
        if type(data) is str:
            return logging.error('Failed to filter the data')
        movies_table = Movies()

        # Create  DB relationships between the tables(week_top and movies)
        for item in data:
            self._conn = sqlite3.connect('imdb.db')
            movies_table.insert_movies(item['name'])
            movies_id = movies_table._conn.execute('SELECT last_insert_rowid()').fetchone()[0]

            cursor = self._conn.cursor()
            try:
                cursor.execute(f'''
                           INSERT INTO {self.table_name} (id, name, category, rank, votes, rating, release, movies_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ''', (
                    item['id'],
                    item['name'],
                    item['category'],
                    item['rank'],
                    item['votes'],
                    item['rating'],
                    item['release'],
                    movies_id
                ))
            except sqlite3.IntegrityError:
                logging.warning(f"This item[{item}] is already in the database")

            self._conn.commit()
            self._conn.close()


# Set class for Whats Streaming endpoint
class WhatsStreaming(IMDBdata):
    def __init__(self):
        super().__init__('streaming')
        self.create_table(id='TEXT PRIMARY KEY',
                          name='TEXT',
                          category='TEXT',
                          rank='INTEGER',
                          votes='INTEGERS',
                          rating='REAL',
                          release='TEXT',
                          provider='TEXT')
        self.endpoint = endpoints.get('get_whats_streaming')

    # Normalization data
    def filter_data(self) -> list[dict[str, str | int | None | float | Any]]:
        data = self.get_data()
        if type(data) is str:
            return logging.error('Failed to filter the data')

        list_filtered_data = list()
        for item in data['data']:
            filtered_data = dict()

            filtered_data.update({'id': item['edges'][0]['title']['id']})


            filtered_data['name'] = item['edges'][0]['title']['originalTitleText']['text']


            if item['edges'][0]['title']['ratingsSummary']['topRanking'] is not None:
                filtered_data['rank'] = int(item['edges'][0]['title']['ratingsSummary']['topRanking']['rank'])
            else:
                filtered_data['rank'] = None

            filtered_data['votes'] = int(item['edges'][0]['title']['ratingsSummary']['voteCount'])

            if item['edges'][0]['title']['ratingsSummary']['aggregateRating'] is not None:
                filtered_data['rating'] = float(item['edges'][0]['title']['ratingsSummary']['aggregateRating'])
            else:
                filtered_data['rating'] = None


            filtered_data['category'] = item['edges'][0]['title']['titleType']['categories'][0]['value']
            filtered_data['release'] = item['edges'][0]['title']['releaseYear']['year']
            filtered_data['provider'] = item['providerName']

            list_filtered_data.append(filtered_data)

        return list_filtered_data

    def insert_data_into_db(self) -> None:
        data = self.filter_data()
        for item in data:
            self._conn = sqlite3.connect('imdb.db')
            cursor = self._conn.cursor()
            try:
                cursor.execute(f'''
                           INSERT INTO {self.table_name} (id, name, category, rank, votes, rating, release, provider)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                       ''', (
                    item['id'],
                    item['name'],
                    item['category'],
                    item['rank'],
                    item['votes'],
                    item['rating'],
                    item['release'],
                    item['provider']
                ))
            except sqlite3.IntegrityError:
                logging.warning(f"This item[{item}] is already in the database")
            self._conn.commit()
            self._conn.close()


# Set a class to relate with week_top table
class Movies(IMDBdata):
    def __init__(self):
        super().__init__('movies')
        self.create_table(movies_id='INTEGER PRIMARY KEY AUTOINCREMENT',
                          name='TEXT NOT NULL')

    # Insert values in the movies table
    def insert_movies(self, name):
        self._conn = sqlite3.connect('imdb.db')
        cursor = self._conn.cursor()
        cursor.execute('''
            INSERT INTO movies (name) 
            VALUES (?)
        ''', (name,))
        self._conn.commit()
        cursor.close()


if __name__ == '__main__':

    # Create object for each table
    data_fan = FavoritesFan()
    data_fan.insert_data_into_db()
    week_top = WeekTop()
    week_top.insert_data_into_db()
    streaming = WhatsStreaming()
    streaming.insert_data_into_db()

    conn = sqlite3.connect('imdb.db')

    # Query tables
    df_fan = pd.read_sql_query("SELECT * FROM fan_favorites", conn)
    df_streaming = pd.read_sql_query("SELECT * FROM streaming", conn)

    # Convert the date and time to a datetime format
    df_fan['trailer'] = pd.to_datetime(df_fan['trailer'], errors='coerce')
    df_streaming['release'] = pd.to_datetime(df_streaming['release'], format='%Y', errors='coerce')

    # Calculate 2 KPIs using .agg in your DataFrame
    kpis = df_fan.agg({
        'rating': ['mean'],  # Media rating-urilor
        'votes': ['sum']  # Numărul total de voturi
    })

    # Generate a report after pandas manipulation
    with open('pandas_report.txt', 'w') as report:
        report.write('Converted in datetime format:\n')
        report.write(str(df_fan[['name', 'trailer']].head()))
        report.write('\n')
        report.write(str(df_streaming[['name', 'release']]))
        report.write("\nKPIs calculated:")
        report.write('\n')
        report.write(str(kpis))
    report.close()

    logger.info("Panda's report generated")

    conn.close()
    generate_report_top_week_list()
