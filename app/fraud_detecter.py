import pandas as pd
import sql_scripts as sql
from psycopg2.extras import execute_values
from psycopg2.extensions import connection, cursor
from settings import DIM_PREFIX, FACT_PREFIX


class FraudDetecter:
    def __init__(self, conn: connection, curs:cursor, code: str, schema: str, sourse_schema: str) -> None:
        '''
        Класс для составления отчета мошеннических операций. 

        Args:
            conn (connection) - объект подключения к базе данных\n
            curs (cursor) - курсор для базы данных\n
            code (str) - личный четырехбуквенный код\n
            schema (str) - название схемы для загрузки таблиц\n
            sourse_schema (str) - название схемы хранящей таблицы для отчетов\n
        Returns:
            None
        '''
        self.conn = conn
        self.curs = curs
        self.code = code
        self.schema = schema
        self.sourse_schema = sourse_schema
        self.load_date = None
        # формируем полный код для таблиц
        self.full_code = f'{self.schema}.{self.code}'
        # создаем мета таблицу
        sql.init_meta(self.curs, self.full_code)
        self.conn.commit()


    def load_data(self, date_group: list) -> None:
        '''
        Функция для загрузки данных из файлов в базу данных.

        Args:
            date_group (list) - список с названиями файлов и датой загрузки
        Returns:
            None
        '''
        for i in range(len(date_group)):
            # распаковываем название файла и дату загрузки
            file_name, fdate = date_group[i]
            # переопределяем дату загрузки
            self.load_date = fdate
            # получаем название таблицы и расширение файла
            table_name, date_ext = file_name.rsplit('_', 1)
            f_ext = date_ext.rsplit('.', 1)[1]
            # удаляем временные таблицы
            self.delete_stg_tables(table_name)
            if 'terminals' in file_name:
                # формируем типы данных для полей
                fields_dtype = {
                    'terminal_id': 'varchar(100)',
                    'terminal_type': 'varchar(100)',
                    'terminal_city': 'varchar(100)',
                    'terminal_address': 'varchar(100)'
                }
                # определяем префис
                prefix = DIM_PREFIX
                # получаем имя таргет таблицы
                trg_name = self.init_target_name(prefix, table_name)
                # получаем имя таргет таблицы без указания схемы и заполняем мета таблицу
                tg_name = trg_name.split('.', 1)[1]
                sql.insert_meta(self.curs, tg_name, self.full_code, self.schema)
                self.conn.commit()
                # загружаем файл в таличный вид
                self.pfiles2sql(file_name, table_name, f_ext, fields_dtype)
                # создаем таргет таблицу
                self.init_target_table_hist(
                    table_name,
                    trg_name,
                    pk='terminal_id',
                    fields_dtype=fields_dtype,
                    
                )
            elif 'transactions' in file_name:
                # формируем типы данных для полей
                fields_dtype = {
                    'trans_id': 'varchar(200)',
                    'trans_date': f"timestamp",
                    'amt': 'decimal(8, 3)',
                    'card_num': 'varchar(200)',
                    'oper_type': 'varchar(200)',
                    'oper_result': 'varchar(200)',
                    'terminal': 'varchar(200)'
                }
                # определяем префис
                prefix = FACT_PREFIX
                # получаем имя таргет таблицы
                trg_name = self.init_target_name(prefix, table_name)
                # получаем имя таргет таблицы без указания схемы и заполняем мета таблицу
                tg_name = trg_name.split('.', 1)[1]
                sql.insert_meta(self.curs, tg_name, self.full_code, self.schema)
                self.conn.commit()
                # загружаем файл в таличный вид
                self.pfiles2sql(file_name, table_name, f_ext, fields_dtype)
                # создаем таргет таблицу
                self.init_target_table_hist(
                    table_name,
                    trg_name,
                    pk='trans_id',
                    fields_dtype=fields_dtype,
                    tr=True
                )
            elif 'passport_blacklist' in file_name:
                # формируем типы данных для полей
                fields_dtype = {
                    'entry_dt': "date",
                    'passport_num': 'varchar(200)'
                }
                # определяем префис
                prefix = FACT_PREFIX
                # получаем имя таргет таблицы
                trg_name = self.init_target_name(prefix, table_name)
                # получаем имя таргет таблицы без указания схемы и заполняем мета таблицу
                tg_name = trg_name.split('.', 1)[1]
                sql.insert_meta(self.curs, tg_name, self.full_code, self.schema)
                self.conn.commit()
                # загружаем файл в таличный вид
                self.pfiles2sql(file_name, table_name, f_ext, fields_dtype)
                # создаем таргет таблицу
                self.init_target_table_hist(
                    table_name,
                    trg_name,
                    pk='passport_num',
                    fields_dtype=fields_dtype,
                )
        

    def delete_stg_tables(self, table_name: str) -> None:
        '''
        Функция для удаления временных таблиц

        Args:
            table_name (str) - название таблицы
        '''
        self.curs.execute(f'drop table if exists {self.full_code}_stg_{table_name}')
        self.curs.execute(f'drop table if exists {self.full_code}_stg_source_{table_name}')
        self.curs.execute(f'drop table if exists {self.full_code}_stg_new_{table_name}')
        self.curs.execute(f'drop table if exists {self.full_code}_stg_upd_{table_name}')
        self.curs.execute(f'drop table if exists {self.full_code}_stg_del_{table_name}')
        self.conn.commit()


    def init_target_name(self, prefix: str, table_name: str) -> str:
        '''
        Функция для создания названия таргет таблицы

        Args:
            prefix (str) - префикс для таблицы
            table_name (str) - название таблицы
        Returns:
            target_name (str) - сформированное название таргет таблицы
        '''
        target_name = f'{self.full_code}_{prefix}_{table_name}'
        target_name += f'_hist' if prefix==DIM_PREFIX else ''
        return target_name


    def pfiles2sql(self, file_path: str, table_name: str, file_ext: str, fields_dtype: dict) -> None:
        '''
        Функция для загрузки данных из файлов в табличный вид

        Args:
            file_path (str) - название файла\n
            table_name (str) - название таблицы\n
            file_ext (str) - расширение файла\n
            fields_dtype (dict) - словарь с полями и типами данных\n
        
        Returns:
            None
        '''
        # открываем файл и преобразуем данные из него в датафрейм
        file_path = 'data/' + file_path
        if file_ext == 'xlsx':
            df = pd.read_excel(file_path, index_col=0)
        elif file_ext == 'txt':
            df = pd.read_csv(file_path, delimiter=';', index_col=0)
        else:
            raise ValueError(f'Тип файлов {file_ext} не поддерживается')
        # создаем название для таблицы с исходными данными
        table_name = f'{self.full_code}_stg_source_{table_name}'

        # соединяем поля с их типом данных и находим decimal значения
        fields_dts = []
        decimal_indx = []
        for i, field_dtype in enumerate(fields_dtype.items()):
            field, dtype = field_dtype
            fields_dts.append(f'{field} {dtype}')
            if 'decimal' in dtype:
                decimal_indx.append(i)

        # соединяем поля с типами данных в одну строку и создаем sql скрипт
        fields_str = ',\n\t'.join(fields_dts)
        create_table_query = f'create table {table_name} ({fields_str}, update_dt timestamp);'
        self.curs.execute(create_table_query)
        self.curs.execute(f"select to_date('{self.load_date}', 'DDMMYYYY') + current_time")
        load_date = self.curs.fetchone()

        # в каждой строке меняем decimal значения. добавляем строки для заполнения в values
        values = []
        for i, row in enumerate(df.itertuples(index=True, name=None)):
            row = list(row)
            if decimal_indx:
                for di in decimal_indx:
                    row[di] = float(row[di].replace(',', '.'))
            row.append(load_date)
            values.append(row)

        # создаем и выполняем запрос на заполнение данных в таблицу с исходными данными
        insert_query = f'''
        insert into {table_name}
        values %s
        '''
        execute_values(
            self.curs, insert_query, values
        )
        self.conn.commit()


    def init_target_table_hist(self, table_name: str, target_name: str, pk: str, fields_dtype: dict, tr: bool = False) -> None:
        '''
        Функция для инициализации и заполнения таргет таблиц.

        Args:
            table_name (str) - название таблицы\n
            target_name (str) - название таргет таблицы\n
            pk (str) - первичный ключ в таблице\n
            fields_dtype (dict) - словарь с поляи и типами данных\n
            tr (bool = False) (optional) - флаг обозначающий загрузку транзакций\n

        Rerurns:
            None
        '''
        # формируем списки с полями и с полями и типами данных. объединяем каждый в одну строку
        fields_dts = []
        fields = []
        for field, dtype in fields_dtype.items():
            fields_dts.append(f'{field} {dtype}')
            fields.append(field)
        fields_dts_str = ',\n\t'.join(fields_dts)
        fields_str = ',\n\t'.join(fields)

        # скрипт для преобразования даты загрузки в тип данных date
        load_date = f"to_date('{self.load_date}', 'DDMMYYYY') + current_time"
        # создаем таргет таблицу
        sql.target_hist(self.curs, target_name, fields_dts_str, load_date)
        # создаем представление хранящее актуальные данные
        view_name = f'{self.full_code}_v_{table_name}'
        sql.view(self.curs, view_name, fields_str, target_name, load_date)
        self.conn.commit()

        if tr:
            # заполняем таблицу с транзакциями
            sql.insert_transactions(self.curs, target_name, fields_str, self.full_code, table_name, load_date)
        else: 
            # создаем временные таблицы и обновляем данные в таргет таблице
            self.create_change_tables(table_name, fields, pk)
            self.update_table_hist(table_name, target_name, fields_str, pk, load_date)
            # self.print_results(table_name)
        self.conn.commit()
        # получаем название таргет таблицы без схемы и обновляем мета таблицу
        trg_name = target_name.split('.', 1)[1]
        sql.update_meta(self.curs, trg_name, self.full_code, self.schema, table_name)
        self.conn.commit()
        

    def create_change_tables(self, table_name: str, fields: list, pk: str) -> None:
        '''
        Функция для создания и временных таблиц с данными об изменениях в таргет таблице

        Args:
            table_name (str) - называние таблицы\n
            fields (list) - список с полями в таблице\n
            pk (str) - первичный ключ таблицы\n

        Returns:
            None
        '''
        # создаем строку со всеми полями с указателем stg
        all_fields = ',\n\t'.join([f'stg.{f}' for f in fields])
        # создаем временную таблицу с новыми данными в таргет таблице
        sql.scd_new(self.curs, all_fields, self.full_code, table_name, pk)
        # создаем строку с выражением неравенства всех полей stg и trg
        fields_ne_cond = '\n\t'.join([f'or stg.{f} != trg.{f}' for f in fields])
        # создаем временную таблицу с измененными данными в таргет таблице
        sql.scd_upd(self.curs, all_fields, self.full_code, table_name, pk, fields_ne_cond)
        # создаем временную таблицу с удаленными данными в таргет таблице
        sql.scd_del(self.curs, table_name, self.full_code, pk)
        self.conn.commit()


    def update_table_hist(self, table_name: str, target_name: str, fields_str: list, pk: str, load_date) -> None:
        '''
        Функция для изменения таргет таблице на основе данных в временных таблицах
        
        Args:
            table_name (str) - название таблицы\n
            target_name (str) - название таргет таблицы\n
            fields_str (list) - строка с полями таблицы\n
            pk (str) - первичный клч таблицы\n
            load_date (str) - скрипт на выборку даты загрузки\n
        
        Returns:
            None
        '''
        sql.target_update_new(self.curs, table_name, target_name, fields_str, load_date, self.full_code)
        sql.target_update_upd(self.curs, table_name, target_name, fields_str, load_date, pk, self.full_code)
        sql.target_update_del(self.curs, table_name, target_name, fields_str, load_date, pk, self.full_code)
        self.conn.commit()


    def print_results(self, table_name):
        tables = [
            # 'stg_{t}',
            '{c}_stg_new_{t}',
            '{c}_stg_upd_{t}',
            '{c}_stg_del_{t}',
            # 'target_{t}_hist'
            ]

        for table in tables:
            table = table.format(t=table_name, c='deit.anka')
            self.curs.execute(f'''select * from {table} ''')
            with open(f'out_{table_name}.txt', 'a+', encoding='utf-8') as f:
                header = '_-' * 4 + table + '-_' * 4
                f.write(f'{header}\n')
                for row in self.curs.fetchall():
                    f.write(f'{row}\n')
                footer = '_-'*10
                f.write(footer + '\n')


    def rep_fraud(self):
        ''' Функция для создания и заполнения таблицы отчетов'''
        sql.init_rep_fraud(self.curs, self.full_code)
        sql.insert_rep_fraud(self.curs, self.full_code, self.load_date)
        self.conn.commit()
        

    def __enter__(self):
            return self
    

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.curs.close()
