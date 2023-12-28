import psycopg2
from psycopg2.extensions import connection, cursor
from settings import DATABASE, HOST, PASSWORD, PORT, USER


def init_db_conn() -> tuple[connection, cursor]:
    """
    Функция для иницилизации подключения к базе данных используя параметры из файла настроек

    Returns:
        tuple[connection, cursor] - кортеж с подключением и курсором к базе данных
    """
    conn = psycopg2.connect(
        database=DATABASE, host=HOST, user=USER, password=PASSWORD, port=PORT
    )
    # Отключение автокоммита
    conn.autocommit = False
    cursor = conn.cursor()

    return conn, cursor
