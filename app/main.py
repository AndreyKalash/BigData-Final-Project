import os
from fraud_detecter import FraudDetecter
from db_conn import init_db_conn
from time import sleep
from settings import CODE, SCHEMA, SOURSE_SCHEMA
import shutil


conn, curs = init_db_conn()

# получаем названия всез файлов в папке дата
files_list = os.listdir('data')
file_groups = {}
# для каждого файла, достаем название, дату и расширение файла. после добавляем в каждую категорию соответсвующие названия файлов
for file in files_list:
    file_name, file_date = file.rsplit('_', 1)
    file_date, file_type = file_date.split('.')
    file_groups.setdefault(file_name, []).append(({file}, file_date))

# для каждой категории файлов сортируем названия файлов по дате
for ftype, file_list in file_groups.items():
    sorted_file_list = sorted(file_list, key=lambda x: x[1])
    file_groups[ftype] = sorted_file_list

# получаем количество файлов из категории в которой больше всего файлов
r_count = len(max(file_groups.values(), key=lambda x: len(x)))

def archive_files(files_group: list) -> None:
    '''
    Функция для перемещения файлов из папки 'data' в папку 'archive'. Создает 'archive' если ее нет

    Args:
        files_group (list) - список с названиями файлов которые нужно перенести в архив
    '''
    # если нет папки архив - создает ее
    if not os.path.exists('archive'):
        os.makedirs('archive')

    # для каждого файла добавляется .backup и перенос в архив
    for file_name in files_group:
        file_name = file_name[0]
        src_path = os.path.join('data', file_name)
        dest_path = os.path.join('archive', f"{file_name}.backup")

        shutil.move(src_path, dest_path)
        print(f"Файл {file_name} перемещен в {dest_path}")

# используя FraudDetecter загружаем данные и составляем отчет мон=шенников
with FraudDetecter(conn, curs, CODE, SCHEMA, SOURSE_SCHEMA) as fd:
    for i in range(r_count):
        # имитация ежедневной загрузки 
        sleep(1)
        # составляем список с названием файла и датой
        date_group = [(list(file_list[i][0])[0], file_list[i][1]) for file_list in file_groups.values()]
        # загружаем выбранные файлы
        fd.load_data(date_group)
        # заполняем отчет
        fd.rep_fraud()
        # переносим файлы в архив
        # archive_files(date_group)