import os
from typing import List

from config import DB_URL  # параметры подключения к БД из модуля конфигурации config.py
from .repository import sql_api  # подключаем API для работы с БД
from .repository.connectorfactory import SQLStoreConnectorFactory

from datetime import datetime

"""
    В данном модуле реализуются бизнес-логика обработки клиентских запросов.
    Здесь также могут применяться SQL-методы, представленные в модуле repository.sql_api
"""


def get_source_files_list() -> List[tuple]:
    """ Получаем список обработанных файлов """
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)  # инициализируем соединение
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_all_from_source_files(db_connector)  # получаем список всех обработанных файлов
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result


def get_processed_data(source_file: int, asset=None) -> List[dict]:
    """ Получаем обработанные данные из основной таблицы """
    db_connector = SQLStoreConnectorFactory().get_connector(DB_URL)
    db_connector.start_transaction()  # начинаем выполнение запросов (открываем транзакцию)
    result = sql_api.select_rows_from_processed_data(db_connector, source_file=source_file, asset=asset)
    db_connector.end_transaction()  # завершаем выполнение запросов (закрываем транзакцию)
    db_connector.close()
    return result


def get_top100_crypto_dict() -> dict:
    with open("webservice/data/top100_crypto.txt", "r") as f:
        s = f.read().split("\n")
    s = {i + 1: s[i] for i in range(len(s))}
    return s


def get_spread_from_processed_data(source_file: int, asset=None) -> List[dict]:
    if asset is None:
        return [{}]
    data = get_processed_data(source_file, asset)
    # [{id, exchange, asset1, asset2, price}]
    possible_spread_table = []
    added_index_table = {}
    for i in range(0, len(data)):
        for j in range(1, len(data)):
            if added_index_table.get(i) != j:
                if data[i]["asset1"] == data[j]["asset1"]:
                    if data[i]["asset2"] == data[j]["asset2"]:
                        added_index_table[i] = j
                        possible_spread_table.append((data[i], data[j]))
    spread_table = []
    count = 0
    for item in possible_spread_table:
        first_set = item[0]
        second_set = item[1]
        max_price, min_price = max(first_set["price"], second_set["price"]), min(first_set["price"],
                                                                                 second_set["price"])
        spread = (max_price - min_price) / max_price * 100
        if 0 < spread < 10:
            count += 1
            spread_table.append(
                {"count": count, "exchange1": first_set["exchange"], "price1": first_set["price"],
                 "exchange2": second_set["exchange"],
                 "price2": second_set["price"], "asset1": first_set["asset1"],
                 "asset2": second_set["asset2"], "spread": round(spread, 2)})
    print(len(spread_table))
    return spread_table


def check_for_new_files():
    # находит первый найденный еще не записанный в бд файл
    all_files_in_dir = os.listdir(r"D:\5.UNIK\ugatu-software-design\webservice\data")
    all_files_xlsx = [file for file in all_files_in_dir if file.endswith(".xlsx")]
    result = get_source_files_list()
    j = -1
    for i in range(len(result)):
        j += 1
        if result[i][1] != all_files_xlsx[j]:
            print(all_files_xlsx[j])
            return
        else:
            date_db = result[i][2].split(":")
            hour_db = date_db[0].split(" ")[-1]
            minute_db = date_db[-1]
            curr_hour = datetime.now().hour
            curr_min = datetime.now().minute
            if hour_db != curr_hour and minute_db != curr_min:
                print(all_files_xlsx[j])
                return
