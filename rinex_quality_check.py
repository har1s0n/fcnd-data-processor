import re
import os
import logging
from datetime import datetime

current_date = datetime.now().strftime('%Y-%m-%d')
log_filename = f'rinex_quality_check_{current_date}.log'

if not os.path.exists('./logs'):
    os.makedirs('./logs')

logging.basicConfig(filename=f'./logs/{log_filename}', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def is_valid_rinex(file_path):
    errors = []
    file_name = file_path.split('/')[-1]

    # Чтение файла
    try:
        with open(file_path, 'r') as file:
            file_content = file.readlines()
    except Exception as e:
        errors.append(f"Ошибка при чтении файла: {str(e)}")
        return False, errors

    # Поиск строки "END OF HEADER"
    try:
        header_end_index = next(i for i, line in enumerate(file_content) if "END OF HEADER" in line)
    except StopIteration:
        errors.append("Отсутствует строка 'END OF HEADER'.")
        return False, errors

    # Логирование заголовка
    logging.info(f"Проверка файла {file_name}")
    logging.info("Заголовок:")
    for line in file_content[:header_end_index + 1]:
        logging.info(line.strip())

    # Проверка данных спутников начинается после строки "END OF HEADER"
    data_lines = file_content[header_end_index + 1:]
    i = 0
    while i < len(data_lines):
        if data_lines[i].strip() == "":
            i += 1
            continue

        # Проверка формата заголовка данных (первая строка блока)
        header_match = re.match(r'^[A-Z]\d{2}\s+\d{4}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}', data_lines[i])
        if not header_match:
            errors.append(f"Неверный формат заголовка данных: {data_lines[i].strip()}")
            break

        # Проверка значений после заголовка данных
        data_values = re.findall(r'\s*-?\d*\.\d+[DE][+-]\d{2}', data_lines[i])
        if len(data_values) != 3:
            errors.append(f"Неверное количество значений в заголовке данных: {data_lines[i].strip()}")
            break

        # Проверка следующих 3 строк по 4 столбца данных
        for j in range(1, 4):
            if i + j >= len(data_lines):
                errors.append("Недостаточно строк данных для проверки.")
                break

            # Извлечение значений столбцов
            columns = re.findall(r'\s*-?\d*\.\d+[DE][+-]\d{2}', data_lines[i + j])
            if len(columns) != 4:
                errors.append(f"Неверное количество столбцов данных: {data_lines[i + j].strip()}")
                break

            # Проверка на аномально большие или маленькие числа
            for col_idx, value in enumerate(columns):
                number = float(value.replace('D', 'E'))
                if col_idx == 0:  # Пределы для псевдодальности (км)
                    if number != 0 and (number > 4e4 or number < -4e4):
                        errors.append(f"Аномально большое или маленькое значение псевдодальности: {value}")
                elif col_idx == 1:  # Пределы для скорости (км/с)
                    if number != 0 and (number > 3e1 or number < -3e1):
                        errors.append(f"Аномально большое или маленькое значение скорости: {value}")
                elif col_idx == 2:  # Пределы для ускорения (км/с²)
                    if number != 0 and (number > 3e-2 or number < -3e-2):
                        errors.append(f"Аномально большое или маленькое значение ускорения: {value}")
                elif col_idx == 3:
                    pass

        i += 4

    return len(errors) == 0, errors


def log_results(file_name, status, errors):
    if status:
        logging.info(f"Файл {file_name} валидный.")
    else:
        logging.error(f"Файл {file_name} невалидный. Ошибки:")
        for error in errors:
            logging.error(f" - {error}")
