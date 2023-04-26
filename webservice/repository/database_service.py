from .connectorfactory import SQLStoreConnectorFactory
import sql_api
"""
    В данном модуле реализуется класс с основной бизнес-логикой приложения. 
    Обычно такие модули / классы имеют в названии слово "Service".
"""


class DataBaseService:

    def __init__(self, datasource: str):
        self.datasource = datasource
        # Инициализируем в конструкторе фабрику DataProcessor
        self.database_fabric = SQLStoreConnectorFactory()
        self.sqlite_store_connector = None

    def run_service(self) -> None:
        database = self.database_fabric.get_connector(self.datasource)        # Инициализируем обработчик
        self.sqlite_store_connector = database
        if database is not None:
            database.check_db_exists()
            database.connect()
        else:
            print('Nothing to run')

    def save_to_db(self, result):
        self.sqlite_store_connector.start_transaction()
        sql_api.insert_rows_into_processed_data(self.sqlite_store_connector, result)
        self.sqlite_store_connector.end_transaction()


