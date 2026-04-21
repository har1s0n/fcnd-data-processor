import requests
import argparse
import datetime
import concurrent.futures
from rinex_merger import RinexMerger
from typing import List, Dict, Any
import configparser
import pandas as pd
import matplotlib
import platform
import subprocess
import zipfile
import gzip
import shutil
import os
import re
from pathlib import Path

matplotlib.use('Agg')
from matplotlib import pyplot as plt


def get_collection_id(collection_name: str) -> int:
    """
    Получает идентификатор коллекции по имени.

    Args:
        collection_name (str): Иимя коллекции.

    Returns:
        int: Идентификатор коллекции.

    Raises:
        ValueError: Если коллекция с заданным именем не найдена.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    response = requests.get("https://fcnd.ru/api/getFilter/", headers=headers)
    response.raise_for_status()
    data = response.json()
    fields = data['answer']['t_meta_collection']['Fields']
    for item in fields:
        if item['c_short_name'] == collection_name:
            return item.get("pk_id")
    raise ValueError(f"Collection '{collection_name}' not found.")


def get_files_list(collection_id: int, dt_begin: str, dt_end: str) -> List[Dict[str, Any]]:
    """
    Получает список файлов для заданной коллекции и периода времени.

    Args:
        collection_id (int): Идентификатор коллекции.
        dt_begin (str): Начальная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
        dt_end (str): Конечная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.

    Returns:
        List[Dict[str, Any]]: Список файлов.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    url = "https://fcnd.ru/api/getData/"
    params = {
        "filter[time_begin]": dt_begin,
        "filter[time_end]": dt_end,
        "filter[meta_collection][]": collection_id
    }
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def filter_files(files_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
     Фильтрует список файлов на основе заданных шаблонов.

     Args:
         files_list (List[Dict[str, Any]]): Список файлов для фильтрации.

     Returns:
         List[Dict[str, Any]]: Отфильтрованный список файлов.
     """
    file_patterns = [
        r"\.\d{2}g\.Z$",
        r"RN\.rnx",
        r"\.RNX\.gz$",
        lambda filename: filename.endswith(".zip") and not re.search(r"\.rnx", filename)
    ]
    # return [file for file in files_list if any(
    #     re.search(pattern, file["pk_file_name"]) if isinstance(pattern, str) else pattern(file["pk_file_name"]) for
    #     pattern in file_patterns)]

    return [
        file for file in files_list
        # Проверяем наличие EKBG в имени файла (приводим к верхнему регистру для надежности)
        if "EKBG" in file.get("pk_file_name", "").upper() and any(
            re.search(pattern, file["pk_file_name"]) if isinstance(pattern, str) else pattern(file["pk_file_name"])
            for pattern in file_patterns
        )
    ]


def download_file(file: Dict[str, Any], download_dir: str) -> str:
    """
    Загружает файл по его параметрам и сохраняет в указанную директорию.

    Args:
        file (Dict[str, Any]): Информация о файле для загрузки.
        download_dir (str): Путь к директории для сохранения файла.

    Returns:
        str: Имя загруженного файла.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }
    time_begin = file["pt_time_begin"]
    file_name = file["pk_file_name"]
    url = f"https://fcnd.ru/api/getData/"
    params = {
        "datafile[time_begin]": time_begin,
        "datafile[file_name]": file_name
    }
    response = requests.get(url, headers=headers, params=params, stream=True)
    response.raise_for_status()
    file_path = os.path.join(download_dir, file_name)
    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Downloaded: {file_name}")
    return file_name


def get_unix_path(win_path):
    return win_path.replace('\\', '/')


def get_win_path(unix_path):
    unix_path = unix_path.replace('/', '\\')
    if unix_path.startswith('\\'):
        win_path = unix_path
    else:
        drive_letter = os.path.splitdrive(os.getcwd())[0]
        win_path = f'{drive_letter}{unix_path}'

    return win_path


def extract_file(file_name: str, download_dir: str) -> str:
    """
    Распаковывает архивный файл и удаляет его после успешной распаковки.

    Args:
        file_name (str): Имя файла для распаковки.
        download_dir (str): Путь к директории, где находится файл.

    Returns:
        str: Имя распакованного файла.
    """
    file_path = os.path.join(download_dir, file_name)
    extracted_file_name = None

    if file_name.endswith(".zip"):
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            extracted_files = zip_file.namelist()
            if len(extracted_files) == 1:
                zip_file.extractall(download_dir)
                extracted_file_name = extracted_files[0]
            else:
                for extracted_file in extracted_files:
                    if re.search(r"\.\d{2}g", extracted_file):
                        zip_file.extract(extracted_file, download_dir)
                        extracted_file_name = extracted_file
                        break
        os.remove(file_path)

    elif file_name.endswith(".Z"):
        if platform.system().lower() == "windows":
            subprocess.run(
                f"\"C:\\Program Files\\Git\\bin\\bash.exe\" -c \"gunzip -f {get_unix_path(os.path.abspath(file_path))}\"",
                shell=True)
        else:
            subprocess.run(f"gunzip -f {file_path}", shell=True)
        extracted_file_name = file_name[:-2]

    elif file_name.endswith(".gz"):
        extracted_file_name = file_name[:-3]
        extracted_file_path = os.path.join(download_dir, extracted_file_name)

        with gzip.open(file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        os.remove(file_path)

    return extracted_file_name


def extract_info_from_rinex3(filename: str) -> (str, int, int):
    """
    Извлекает имя станции, год и день в году из имени файла Rinex 3.

    Args:
        filename (str): Имя файла Rinex 3.

    Returns:
        tuple: Имя станции, год (YYYY) и день в году (DOY).
    """
    base_name = os.path.basename(filename)
    station_name = base_name[:4]
    year_str = base_name.split('_')[2][:4]
    doy_str = base_name.split('_')[2][4:7]
    year_full = int(year_str)
    doy = int(doy_str)
    return station_name, year_full, doy


def extract_info_from_rinex2(input_file: str):
    filename = os.path.basename(input_file)
    station_name = filename[:4]
    year = int(filename[9:11]) + 2000
    day_of_year = int(filename[4:7])
    return station_name, year, day_of_year


def convert_rinex3_nav_to_rinex2(input_file: str, output_dir: str) -> str:
    """
    Конвертирует файл Rinex 3 навигационных данных в Rinex 2 с использованием утилиты convbin.

    Args:
        input_file (str): Путь к входному файлу Rinex 3.
        output_dir (str): Директория для сохранения выходного файла Rinex 2.

    Returns:
        str: Путь к выходному файлу Rinex 2.
    """
    system = platform.system().lower()

    convbin_executable = ""
    if system == "windows":
        convbin_executable = os.path.join("executables", "convbin_win", "convbin.exe")
    elif system == "linux":
        convbin_executable = os.path.join("executables", "convbin_linux", "convbin")
    elif system == "darwin":  # macOS
        convbin_executable = os.path.join("executables", "convbin_mac", "convbin")
    else:
        raise OSError("Unsupported operating system")

    convbin_executable = os.path.abspath(convbin_executable)
    input_file = os.path.abspath(input_file)

    if not os.path.isfile(convbin_executable):
        raise FileNotFoundError(f"convbin executable not found: {convbin_executable}")

    station_name, year_full, day_of_year = extract_info_from_rinex3(input_file)
    year_suffix = str(year_full)[-2:]

    output_file_name = f"{station_name.lower()}{day_of_year:03d}0.{year_suffix}g"
    output_file = os.path.join(output_dir, output_file_name)

    command = [convbin_executable, input_file, "-r", "rinex", "-n", output_file]

    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Conversion output: {result.stdout.decode('utf-8')}")
        print(f"Conversion errors: {result.stderr.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e.stderr.decode('utf-8')}")
        raise

    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"Output file not found: {output_file}")

    return output_file


def convert_rinex2_nav_to_rinex3(input_file: str, output_dir: str) -> str:
    """
    Конвертирует файл Rinex 2 в Rinex 3 с использованием утилиты convbin и вносит необходимые изменения в заголовок.

    Args:
        input_file (str): Путь к входному файлу Rinex 2.
        output_dir (str): Директория для сохранения выходного файла Rinex 3.

    Returns:
        str: Путь к выходному файлу Rinex 3.
    """
    system = platform.system().lower()

    convbin_executable = ""
    if system == "windows":
        convbin_executable = os.path.join("executables", "convbin_win", "convbin.exe")
    elif system == "linux":
        convbin_executable = os.path.join("executables", "convbin_linux", "convbin")
    elif system == "darwin":  # macOS
        convbin_executable = os.path.join("executables", "convbin_mac", "convbin")
    else:
        raise OSError("Unsupported operating system")

    convbin_executable = os.path.abspath(convbin_executable)
    input_file = os.path.abspath(input_file)

    if not os.path.isfile(convbin_executable):
        raise FileNotFoundError(f"convbin executable not found: {convbin_executable}")

    station_name, year_full, day_of_year = extract_info_from_rinex2(input_file)

    output_file_name = f"{station_name.upper()}00RUS_R_{year_full}{day_of_year:03d}0000_01D_RN.rnx"
    output_file = os.path.join(output_dir, output_file_name)

    command = [convbin_executable, "-r", "rinex", "-v", "3.04", "-n", os.path.abspath(output_file), input_file]

    try:
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Conversion output: {result.stdout.decode('utf-8')}")
        print(f"Conversion errors: {result.stderr.decode('utf-8')}")
    except subprocess.CalledProcessError as e:
        print(f"Error during conversion: {e.stderr.decode('utf-8')}")
        raise

    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"Output file not found: {output_file}")

    # Чтение второго файла для вставки второй строки
    with open(input_file, 'r') as infile:
        rinex2_lines = infile.readlines()
    second_line = rinex2_lines[1].strip()

    # Внесение изменений в заголовок RINEX 3
    with open(output_file, 'r+') as outfile:
        lines = outfile.readlines()
        lines[0] = "     3.04           N: GNSS NAV DATA    R: GLONASS          RINEX VERSION / TYPE\n"
        lines[1] = second_line + "\n"
        lines.insert(2, "    18                                                      LEAP SECONDS\n")
        outfile.seek(0)
        outfile.writelines(lines)

    return output_file


def read_config(config_dir: str):
    if not os.path.isfile(os.path.join(config_dir, 'config.ini')):
        print('Config file not found')
        exit(0)
    config = configparser.ConfigParser()
    config.read(os.path.join(config_dir, 'config.ini'))

    return config


def handle_file(file_name: str, dt_begin: str, download_dir: str) -> str:
    """
    Переименовывает файл: приводит имя станции и расширение к нижнему регистру,
    а также конвертирует файл RINEX 2 в RINEX 3, если это необходимо.

    Args:
        file_name (str): Имя файла.
        dt_begin (str): Начальная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
        download_dir (str): Директория загрузки.

    Returns:
        str: Новое имя файла.
    """
    file_path = Path(download_dir) / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    # Определяем, является ли файл классическим .??g файлом
    if re.search(r"\.\d{2}g$", file_name, re.IGNORECASE):
        new_file_name = f"{file_path.stem.lower()}.{dt_begin[6:10][-2:]}g"
        new_file_path = file_path.with_name(new_file_name)

        file_path.rename(new_file_path)
        final_file_name = new_file_path.name

        convert_rinex2_nav_to_rinex3(str(new_file_path), download_dir)

    else:
        # Обработка файлов типа .RNX
        stem_lower = file_path.stem.lower()
        suffix_lower = file_path.suffix.lower()  # .RNX -> .rnx
        new_file_name = stem_lower + suffix_lower
        new_file_path = file_path.with_name(new_file_name)

        if new_file_path != file_path:
            file_path.rename(new_file_path)

        final_file_name = new_file_path.name

    return final_file_name


def analyze_merge_results(result_df: pd.DataFrame) -> None:
    """
    Анализирует результат работы функции merge_files и строит график отсутствующих измерений.

    Args:
        result_df (pd.DataFrame): DataFrame с объединенными спутниковыми данными.
    """
    plots_dir = './plots'
    if not os.path.exists(plots_dir):
        os.makedirs(plots_dir)

    plt.ioff()
    min_date = result_df['datetime_utc'].min()
    max_date = result_df['datetime_utc'].max()
    all_times = pd.date_range(start=min_date, end=max_date, freq='30min')

    unique_svs = result_df['SV'].unique()

    sv_data = {sv: [] for sv in unique_svs}

    for sv in unique_svs:
        sv_times = result_df[result_df['SV'] == sv]['datetime_utc']
        sv_data[sv] = [time in sv_times.values for time in all_times]

    # Создаем график
    fig, ax = plt.subplots(figsize=(20, 15))
    ax.set_title('Зоны видимости НКА ГНСС ГЛОНАСС')
    ax.set_xlabel('Дата и время')
    ax.set_ylabel('НКА')

    # Отображаем данные на графике
    for idx, sv in enumerate(unique_svs):
        x = all_times
        c = ['green' if available else 'red' for available in sv_data[sv]]
        for i in range(len(all_times) - 1):
            ax.fill_between(x[i:i + 2], y1=idx - 0.4, y2=idx + 0.4, color=c[i])

    ax.set_yticks(range(len(unique_svs)))
    ax.set_yticklabels(unique_svs)
    plt.xticks(rotation=45)
    plt.grid(True)

    green_patch = plt.Line2D([0], [0], linestyle='None', marker='_', color='green', markersize=20,
                             label='Измерение есть')
    red_patch = plt.Line2D([0], [0], linestyle='None', marker='_', color='red', markersize=20,
                           label='Измерение отсутствует')

    plt.legend(handles=[green_patch, red_patch], loc='best', bbox_to_anchor=(0.3, -0.2), ncol=2)
    fig.subplots_adjust(bottom=0.5)

    plt.savefig(os.path.abspath(
        os.path.join(plots_dir, f"visibility_plot_{datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}.png")),
        bbox_inches='tight')


def main(dt_begin: str, dt_end: str) -> None:
    """
    Основная функция, которая управляет процессом загрузки и распаковки файлов.

    Args:
        dt_begin (str): Начальная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
        dt_end (str): Конечная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
    """
    download_dir = "./downloads"
    config_dir = "./config"

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    if not os.path.exists(config_dir):
        print(f"The {config_dir} directory is missing.")
        exit(0)

    config = read_config(config_dir)
    collections = config['FCND']['collections'].split(';')

    try:
        filtered_file_list = list()
        for collection_name in collections:
            collection_id = get_collection_id(collection_name)
            files_list = get_files_list(collection_id, dt_begin, dt_end)
            filtered_file_list.extend(filter_files(files_list))

        with concurrent.futures.ThreadPoolExecutor() as download_executor:
            future_to_file = {download_executor.submit(download_file, file, download_dir): file for file in
                              filtered_file_list}
            download_results = {future: future.result() for future in concurrent.futures.as_completed(future_to_file)}

        with concurrent.futures.ThreadPoolExecutor() as extract_executor:
            extract_futures = [extract_executor.submit(extract_file, archive_name, download_dir) for archive_name in
                               download_results.values()]
            for future in concurrent.futures.as_completed(extract_futures):
                file_name = future.result()
                if file_name:
                    handle_file(file_name, dt_begin, download_dir)

        merger = RinexMerger(download_dir, './brdc')
        brdc_file_dataframe = merger.merge_files('mixed', datetime.datetime.strptime(dt_begin, "%d-%m-%Y %H:%M:%S"),
                                                 datetime.datetime.strptime(dt_end, "%d-%m-%Y %H:%M:%S"))
        analyze_merge_results(brdc_file_dataframe)

    except Exception as e:
        print(f"An error occurred: {e}")


def validate_datetime(datetime_str: str) -> str:
    """
    Валидирует строку с датой и временем, соответствующую формату 'DD-MM-YYYY hh:mm:ss'.

    Args:
        datetime_str (str): Строка с датой и временем для валидации.

    Returns:
        str: Валидированная строка с датой и временем.

    Raises:
        argparse.ArgumentTypeError: Если формат даты и времени неверен.
    """
    pattern = r"^\d{2}-\d{2}-\d{4}\s\d{2}:\d{2}:\d{2}$"
    if not re.match(pattern, datetime_str):
        raise argparse.ArgumentTypeError("Неверный формат даты и времени (требуемый формат: DD-MM-YYYY hh:mm:ss).")
    try:
        datetime.datetime.strptime(datetime_str, "%d-%m-%Y %H:%M:%S")
    except ValueError:
        raise argparse.ArgumentTypeError("Неверный формат даты и времени.")
    return datetime_str


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='File collector from the federal navigation data center FCND')
    parser.add_argument('--dt_begin', type=validate_datetime, dest='dt_begin',
                        default=(datetime.date.today() - datetime.timedelta(days=1)).strftime('%d-%m-%Y %H:%M:%S'),
                        help="start of file collection (format: DD-MM-YYYY hh:mm:ss")
    parser.add_argument('--dt_end', type=validate_datetime, dest='dt_end',
                        default=datetime.date.today().strftime('%d-%m-%Y %H:%M:%S'),
                        help="end of file collection (format: DD-MM-YYYY hh:mm:ss")

    args = parser.parse_args()

    main(args.dt_begin, args.dt_end)
