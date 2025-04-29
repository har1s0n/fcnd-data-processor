import re
import os
import logging
from datetime import datetime

# Настройка логирования
current_date = datetime.now().strftime('%Y-%m-%d')
log_filename = f'rinex_quality_check_{current_date}.log'
os.makedirs('./logs', exist_ok=True)

logging.basicConfig(filename=f'./logs/{log_filename}', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def is_valid_rinex(file_path):
    errors = []
    file_name = os.path.basename(file_path)

    # Системы, которые мы пропускаем (например, GPS, Galileo, BeiDou)
    skip_systems = ['G', 'E', 'C']

    # Правила: сколько строк занимает запись для каждой системы
    system_block_sizes = {
        'G': 8,  # GPS
        'E': 8,  # Galileo
        'C': 8,  # BeiDou
        'R': 4  # ГЛОНАСС
    }

    try:
        with open(file_path, 'r') as file:
            file_content = file.readlines()
    except Exception as e:
        errors.append(f"Ошибка при чтении файла: {str(e)}")
        return False, errors

    # Поиск конца заголовка
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

    # Обработка спутниковых данных
    data_lines = file_content[header_end_index + 1:]
    i = 0
    while i < len(data_lines):
        line = data_lines[i].strip()
        if line == "":
            i += 1
            continue

        # Проверка заголовка спутниковой записи
        header_match = re.match(r'^([A-Z])\s?\d{1,2}\s+\d{4}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}', line)
        if not header_match:
            errors.append(f"Неверный формат заголовка спутника: {line}")
            i += 1
            continue

        system_letter = header_match.group(1).upper()

        block_size = system_block_sizes.get(system_letter)
        if block_size is None:
            errors.append(f"Неизвестная система спутников: {system_letter} в строке: {line}")
            i += 1
            continue

        # Если система в списке пропускаемых — пропускаем весь блок
        if system_letter in skip_systems:
            logging.info(f"Пропущен спутник {system_letter}: {line}")
            i += block_size
            continue

        # Проверяем, хватает ли строк для обработки блока
        if i + block_size - 1 >= len(data_lines):
            errors.append(f"Недостаточно строк данных для спутника {line}")
            break

        # Проверяем данные в блоке
        block_lines = data_lines[i:i + block_size]
        for idx, block_line in enumerate(block_lines):
            block_line = block_line.rstrip()
            floats = re.findall(r'[-\s]?\d*\.\d+[DE][\+\-]\d{2}', block_line)

            # На первой строке (заголовок) обычно минимум 3 значения
            if idx == 0:
                if len(floats) < 3:
                    errors.append(f"Недостаточно значений в заголовке спутника: {block_line}")
            else:
                # Для каждой строки эфемерид (дальше) должно быть 4 значения
                if len(floats) != 4:
                    errors.append(f"Неверное количество данных на строке {idx + 1} спутника: {block_line}")

            # Проверка диапазона значений
            for value in floats:
                try:
                    number = float(value.replace('D', 'E'))
                except ValueError:
                    errors.append(f"Некорректное числовое значение: {value}")
                    continue

                if idx == 0:
                    if abs(number) > 1e6:
                        errors.append(f"Аномально большое значение в заголовке спутника: {value}")
                else:
                    if abs(number) > 1e9:
                        errors.append(f"Аномально большое значение орбитальных параметров: {value}")

        i += block_size  # Переход к следующему спутнику

    return len(errors) == 0, errors


def log_results(file_name, status, errors):
    if status:
        logging.info(f"Файл {file_name} валидный.")
    else:
        logging.error(f"Файл {file_name} невалидный. Ошибки:")
        for error in errors:
            logging.error(f" - {error}")
