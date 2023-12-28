from psycopg2.extensions import cursor
from psycopg2.extras import execute_values


def init_meta(curs: cursor, full_code: str) -> None:
    """
    Функция для создания мета таблицы

    Args:
        curs (cursor) - объект курсора базы данных\n
        full_code (str) - полный код для таблицы\n

    Returns:
        None
    """
    curs.execute(
        f"""
    create table if not exists {full_code}_meta (
        schema_name varchar(100),
        table_name varchar(100),
        max_update_dt timestamp(0)
    )
    """
    )


def insert_meta(curs: cursor, target_name: str, full_code: str, schema: str) -> None:
    """
    Функция для заполнения мета таблицы

    Args:
        curs (cursor) - объект курсора базы данных\n
        target_name (str) - название таргет таблицы\n
        full_code (str) - полный код для таблицы\n
        schema (str) - название схемы где создается таргет таблица

    Returns:
        None
    """
    curs.execute(
        f"""
    insert into {full_code}_meta (schema_name, table_name, max_update_dt)
    select
    '{schema}',
    '{target_name}',
    to_date('1900-01-01', 'YYYY-MM-DD')
    where not exists (
        select * from {full_code}_meta
        where schema_name = '{schema}' and table_name = '{target_name}'
    );
    """
    )


def update_meta(
    curs: cursor, target_name: str, full_code: str, schema: str, table_name: str
) -> None:
    """
    Функция для изменения мета таблицы

    Args:
        curs (cursor) - объект курсора базы данных\n
        target_name (str) - название таргет таблицы\n
        full_code (str) - полный код для таблицы\n
        schema (str) - название схемы где создается таргет таблица\n
        table_name (str) - название таблицы

    Returns:
        None
    """
    curs.execute(
        f"""
    update {full_code}_meta
        set max_update_dt = (select max(update_dt) from {full_code}_stg_source_{table_name})
        where schema_name = '{schema}' and table_name = '{target_name}';
    """
    )


def scd_new(
    curs: cursor, all_fields: str, full_code: str, table_name: str, pk: str
) -> None:
    """
    Функция для создания временной таблицы с новыми записями

    Args:
        curs (cursor) - объект курсора базы данных\n
        all_fields (str) - все поля с указателем stg\n
        full_code (str) - полный код для таблицы\n
        table_name (str) - название таблицы\n
        pk (str) - первичный ключ в таблице\n

    Returns:
        None
    """
    curs.execute(
        f"""
    create table {full_code}_stg_new_{table_name} as
        select
        {all_fields}
        from {full_code}_stg_source_{table_name} stg
        left join {full_code}_v_{table_name} trg
        on stg.{pk} = trg.{pk}
        where trg.{pk} is null
    """
    )


def scd_upd(
    curs: cursor,
    all_fields: str,
    full_code: str,
    table_name: str,
    pk: str,
    fields_ne_cond: str,
) -> None:
    """
    Функция для создания временной таблицы с измененными записями

    Args:
        curs (cursor) - объект курсора базы данных\n
        all_fields (str) - все поля с указателем stg\n
        full_code (str) - полный код для таблицы\n
        table_name (str) - название таблицы\n
        pk (str) - первичный ключ в таблице\n
        fields_ne_cond (str) - выражение неравенства всех полей stg и trg\n

    Returns:
        None
    """
    curs.execute(
        f"""
    create table {full_code}_stg_upd_{table_name} as
        select
        {all_fields}
        from {full_code}_v_{table_name} trg
        inner join {full_code}_stg_source_{table_name} stg
        on stg.{pk} = trg.{pk}
        where (
        1 = 0
        {fields_ne_cond}
        )
    """
    )


def scd_del(curs: cursor, table_name: str, full_code: str, pk: str) -> None:
    """
    Функция для создания временной таблицы с удаленными записями

    Args:
        curs (cursor) - объект курсора базы данных\n
        full_code (str) - полный код для таблицы\n
        table_name (str) - название таблицы\n
        pk (str) - первичный ключ в таблице\n

    Returns:
        None
    """
    curs.execute(
        f"""
    create table {full_code}_stg_del_{table_name} as
        select
            trg.{pk}
        from {full_code}_v_{table_name} trg
        left join {full_code}_stg_source_{table_name} stg
        on stg.{pk} = trg.{pk}
        where stg.{pk} is null
    """
    )


def insert_transactions(
    curs: cursor,
    target_name: str,
    fields_str: str,
    full_code: str,
    table_name: str,
    load_date: str,
) -> None:
    """
    Функция для заполнения таргет таблицы транзакций

    Args:
        curs (cursor) - объект курсора базы данных\n
        target_name (str) - название таргет таблицы\n
        fields_dts (str) - строка с полями\n
        full_code (str) - полный код для таблицы\n
        table_name (str) - название таблицы\n
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date\n

    Returns:
        None
    """
    curs.execute(
        f"""
    insert into {target_name} (
        {fields_str},
        effective_from
    )
    select
    {fields_str},
    {load_date}
    from {full_code}_stg_source_{table_name}
    """
    )


def target_hist(
    curs: cursor, target_name: str, fields_dts_str: str, load_date: str
) -> None:
    """
    Функция для создания таргет таблицы

    Args:
        curs (cursor) - объект курсора базы данных\n
        target_name (str) - название таргет таблицы\n
        fields_dts_str (str) - строка с полями и типами данных
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date

    Returns:
        None
    """
    curs.execute(
        f"""
        create table if not exists {target_name}(
            {fields_dts_str},
            deleted_flg char(1) default 'N',
            effective_from timestamp default ({load_date}),
            effective_to timestamp default '2999-12-31 23:59:59'
        )
        """
    )


def view(
    curs: cursor, view_name: str, fields_str: str, target_name: str, load_date: str
) -> None:
    """
    Функция для создания представления хранящего актуальные значения

    Args:
        curs (cursor) - объект курсора базы данных\n
        view_name (str) - название представления\n
        fields_str (str) - названия полей\n
        target_name (str) - название таргет таблицы\n
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date\n

    Returns:
        None
    """
    curs.execute(
        f"""
        create or replace view {view_name} as
        select
            {fields_str}
        from {target_name}
        where {load_date} between effective_from and effective_to
        """
    )


def target_update_new(
    curs: cursor,
    table_name: str,
    target_name: str,
    fields_str: str,
    load_date: str,
    full_code: str,
) -> None:
    """
    Функция для заполнения таргет таблицы новыми данными

    Args:
        curs (cursor) - объект курсора базы данных\n
        table_name (str) - название таблицы\n
        target_name (str) - название таргет таблицы\n
        fields_str (str) - названия полей\n
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date\n
        full_code (str) - полный код для таблицы\n

    Returns:
        None
    """
    curs.execute(
        f"""
        insert into {target_name} (
            {fields_str},
            effective_from
        )
        select
        {fields_str},
        {load_date}
        from {full_code}_stg_new_{table_name}
        """
    )


def target_update_upd(
    curs: cursor,
    table_name: str,
    target_name: str,
    fields_str: str,
    load_date: str,
    pk: str,
    full_code: str,
) -> None:
    """
    Функция для заполнения таргет таблицы измененными данными

    Args:
        curs (cursor) - объект курсора базы данных\n
        table_name (str) - название таблицы\n
        target_name (str) - название таргет таблицы\n
        fields_str (str) - названия полей\n
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date\n
        pk (str) - первичный ключ в таблице\n
        full_code (str) - полный код для таблицы\n

    Returns:
        None
    """
    # закрытие сесссии измененной строки
    curs.execute(
        f"""
        update {target_name} set
        effective_to = {load_date} - interval '1 second',
        deleted_flg = 'Y'
        where {pk} in (select {pk} from {full_code}_stg_upd_{table_name})
            and effective_to = '2999-12-31 23:59:59'
        """
    )
    # добавление измененной строки с новыми данными
    curs.execute(
        f"""
        insert into {target_name} (
            {fields_str},
            effective_from
        )
        select
        {fields_str},
        {load_date}
        from {full_code}_stg_upd_{table_name}
        """
    )


def target_update_del(
    curs: cursor,
    table_name: str,
    target_name: str,
    fields_str: str,
    load_date: str,
    pk: str,
    full_code: str,
) -> None:
    """
    Функция для заполнения таргет таблицы удаленными данными

    Args:
        curs (cursor) - объект курсора базы данных\n
        table_name (str) - название таблицы\n
        target_name (str) - название таргет таблицы\n
        fields_str (str) - названия полей\n
        load_date (str) - скрипт для преобразования даты загрузки в тип данных date\n
        pk (str) - первичный ключ в таблице\n
        full_code (str) - полный код для таблицы\n

    Returns:
        None
    """
    # закрытие сесссии удаленной строки
    curs.execute(
        f"""
        update {target_name} set
        effective_to = {load_date} - interval '1 second',
        deleted_flg = 'Y'
        where {pk} in (select {pk} from {full_code}_stg_del_{table_name})
            and effective_to = '2999-12-31 23:59:59'
        """
    )
    # добавление удаленной строки с новым временем
    curs.execute(
        f"""
        insert into {target_name} (
            {fields_str},
            deleted_flg,
            effective_from
        )
        select
        {fields_str},
        'Y',
        {load_date}
        from {target_name}
        where {pk} in (select {pk} from {full_code}_stg_del_{table_name})
            and effective_to != '2999-12-31 23:59:59' and deleted_flg = 'Y'
        """
    )


def init_rep_fraud(curs: cursor, full_code: str) -> None:
    """
    Функция для создания таблицы с отчетами мошеннечиских транзакций

    Args:
        curs (cursor) - объект курсора базы данных\n
        full_code (str) - полный код для таблицы\n

    Returns:
        None
    """
    curs.execute(
        f"""
    create table if not exists {full_code}_rep_fraud (
        event_dt timestamp,
        passport varchar(200),
        fio varchar(200),
        phone varchar(200),
        event_type varchar(200),
        report_dt timestamp default(current_timestamp)
    )
    """
    )


def insert_rep_fraud(curs: cursor, full_code: str, load_date: str) -> None:
    """
    Функция для заполнения данными в таблицу с отчетами мошеннечиских транзакций

    Args:
        curs (cursor) - объект курсора базы данных\n
        full_code (str) - полный код для таблицы\n
        load_date (str) - дата загрузки\n

    Returns:
        None
    """
    # выборка полей используемых в запросах для загрузки данных в таблицу с отчетами
    rep_fraud_fields = """
    trn.trans_date as event_dt,
    cln.passport_num as passport,
    cln.last_name||' '||cln.first_name||' '||cln.patronymic as fio,
    cln.phone as phone,
    '{event_type}' as event_type,
    current_timestamp as report_dt
    """

    # выборка таблицы и джоины таблиц используемых в запросах для загрузки данных в таблицу с отчетами
    transactions_join_tables = """
        deit.anka_dwh_fact_transactions trn
    inner join
        bank.cards crd on trim(crd.card_num) = trn.card_num
    inner join
        bank.accounts acc on acc.account = crd.account
    inner join
        bank.clients cln on acc.client = cln.client_id
    """

    # поиск мошеннеческих операций "Совершение операции при просроченном или заблокированном паспорте"
    curs.execute(
        f"""
    insert into {full_code}_rep_fraud (
        select
            {rep_fraud_fields.format(event_type='Совершение операции при просроченном или заблокированном паспорте')}
        from
            {transactions_join_tables}
        left join
            deit.anka_dwh_fact_passport_blacklist blk on trim(blk.passport_num) = trim(cln.passport_num)
        where
            trn.trans_date::date = to_date('{load_date}', 'DDMMYYYY')
            and
            (trn.trans_date > coalesce(cln.passport_valid_to, '2999-12-31')::date
            or
            (trn.trans_date::date >= blk.entry_dt and blk.passport_num is not null))
    )
    """
    )

    # поиск мошеннеческих операций "Совершение операции при недействующем договоре"
    curs.execute(
        f"""
    insert into {full_code}_rep_fraud (
        select
            {rep_fraud_fields.format(event_type='Совершение операции при недействующем договоре')}
        from
            {transactions_join_tables}
        where
            trn.trans_date::date = to_date('{load_date}', 'DDMMYYYY')
            and
            acc.valid_to::date < trn.trans_date::date
    )
    """
    )

    # поиск мошеннеческих операций "Совершение операций в разных городах в течение одного часа"
    curs.execute(
        f"""
    insert into {full_code}_rep_fraud (
        with ordered_city as (
            select
                trn.trans_date,
                trn.card_num,
                lag(trm.terminal_city) over (partition by trn.card_num order by trn.trans_date) as prev_city,
                trm.terminal_city
            from
                deit.anka_dwh_fact_transactions trn
            inner join
                deit.anka_dwh_dim_terminals_hist trm on trn.terminal = trm.terminal_id
        ),
        ordered_line as (
            select
                trans_date,
                card_num,
                prev_city,
                terminal_city,
                dense_rank() over (partition by card_num order by trans_date) as line_number
            from
                ordered_city
            where terminal_city != prev_city
        )
        select
            {rep_fraud_fields.format(event_type='Совершение операций в разных городах в течение одного часа')}
        from
            {transactions_join_tables}
        inner join
            ordered_line ord on ord.trans_date = trn.trans_date
        where
            ord.trans_date::date = to_date('{load_date}', 'DDMMYYYY')
    )
    """
    )

    # поиск мошеннеческих операций "Попытка подбора суммы"
    # получаем все данные с нужными полями, суммой операции и результатом операции, отсортированные по карте и времени
    curs.execute(
        f"""
    select
        {rep_fraud_fields.format(event_type='Попытка подбора суммы')},
        trn.amt,
        trn.oper_result
    from
        {transactions_join_tables}
    where
        trn.trans_date::date = to_date('{load_date}', 'DDMMYYYY')
    order by
        trn.card_num, event_dt
    """
    )

    # получаем данные выборки
    transactions_per_fio = curs.fetchall()

    i = 0
    values = []
    t_count = len(transactions_per_fio)
    # цикл до тех порЮ пока не проверим все строки
    while i < t_count:
        row = transactions_per_fio[i]
        passport = row[1]
        oper_result = row[-1]
        amt = float(row[-2])
        oper_date = row[0]
        # если статус строки - "REJECT"
        if oper_result == "REJECT":
            j = 1
            # объявляем переменные с последней суммой и датой транзакции
            last_amt = amt
            last_oper_date = row[0]
            # до тех пор пока статус следующей строки - "REJECT"
            while transactions_per_fio[j + i][-1] == "REJECT":
                next_row = transactions_per_fio[j + i]
                new_amt = float(next_row[-2])
                new_oper_date = next_row[0]
                # если сумма следующей транзакции, меньше чем последняя - переопределяем последнюю сумму и дату транзакции
                # иначе = выходим из цикла
                if last_amt > new_amt:
                    last_amt = new_amt
                    last_oper_date = new_oper_date
                    j += 1
                else:
                    i += 1
                    break
            # количество последующих отклоненных строк больше или равно двум
            jm2 = j >= 2
            # статус последней операции - "SUCCESS"
            oper_success = transactions_per_fio[j + i][-1] == "SUCCESS"
            # последняя отклоненная сумма больше следующей суммы транзакции
            more_then_last = last_amt > float(transactions_per_fio[j + i][-2])
            # транзакции производил один и тот же человек
            same_person = passport == transactions_per_fio[j + i][1]
            # разница между первой отклоненной и успешной транзакцией меньше или равна 20 минутам
            time_diff = last_oper_date - oper_date
            in20min = (time_diff.total_seconds() / 60) >= 20.0
            # если все условия истина - добавляем строку с успешной операцией в values
            if jm2 and oper_success and more_then_last and same_person and in20min:
                values.append(transactions_per_fio[j + i][:-2])
                i += j
            else:
                i += j
        else:
            i += 1

    # добавляем в rep_fraud мошеннические операции по подбору суммы
    insert_query = f"insert into {full_code}_rep_fraud values %s"
    execute_values(curs, insert_query, values)
