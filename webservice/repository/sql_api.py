from typing import List

from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""


def select_all_from_source_files(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files ORDER BY processed DESC'
    result = connector.execute(query).fetchall()
    return result


def insert_into_source_files(connector: StoreConnector, filename: str):
    """ Вставка в таблицу обработанных файлов """
    now = datetime.now()  # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")  # преобразуем дату в формат SQL, например, '2022-11-15 22:03:16'
    query = f'INSERT INTO source_files (filename, processed) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    return result


def insert_rows_into_processed_data(connector: StoreConnector, dataframe: DataFrame):
    """ Вставка строк из DataFrame в БД с привязкой данных к последнему обработанному файлу (по дате) """
    rows = dataframe.to_dict('records')
    files_list = select_all_from_source_files(connector)  # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple, например:
    # row = (1, 'seeds_dataset.csv', '2022-11-15 22:03:16'),
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами
    if len(files_list) > 0:
        for row in rows:
            connector.execute(f"""
            INSERT INTO crypto (exchange, asset1, asset2, price, source_file) 
            VALUES ('{row["Exchange"]}', '{row["asset1"]}', '{row["asset2"]}',
             '{row["price"]}', '{last_file_id}')
            """
                              )
        print('Data was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')


def select_rows_from_processed_data(connector: StoreConnector, source_file: int, offset: int) -> List[dict]:
    selected_rows = connector.execute("""
    SELECT * FROM crypto
    """)
    dict_pairs = []
    for item in selected_rows.fetchall():
        dict_pairs.append({"id": item[0],
                           "exchange": item[1],
                           "asset1": item[2],
                           "asset2": item[3],
                           "price": item[4],
                           "source_file": item[5]
                           })
    return dict_pairs


def delete_selected_row(connector: StoreConnector, row: dict):
    connector.execute(f"""
    DELETE FROM crypto WHERE id = {row["id"]};
    """)


def update_selected_row(connector: StoreConnector, values_to_change: dict):
    connector.execute(f"""
    UPDATE crypto set exchange = '{values_to_change["exchange"]}',
    asset1 = '{values_to_change["asset1"]}', asset2 = '{values_to_change["asset2"]}',
    price = '{values_to_change["price"]}', source_file = '{values_to_change["source_file"]}'
    WHERE id = {values_to_change["id"]};
    """)
