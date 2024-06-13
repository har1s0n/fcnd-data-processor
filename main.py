import subprocess
import zipfile
import requests
import argparse
import datetime
import os
import re
import platform
import concurrent.futures
from rinex_merger import RinexMerger
from typing import List, Dict, Any


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
    response = requests.get("https://fcnd.ru/api/getFilter/")
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
    url = "https://fcnd.ru/api/getData/"
    params = {
        "filter[time_begin]": dt_begin,
        "filter[time_end]": dt_end,
        "filter[meta_collection][]": collection_id
    }
    response = requests.get(url, params=params)
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
        lambda filename: filename.endswith(".zip") and not re.search(r"\.rnx", filename)
    ]
    return [file for file in files_list if any(
        re.search(pattern, file["pk_file_name"]) if isinstance(pattern, str) else pattern(file["pk_file_name"]) for
        pattern in file_patterns)]


def download_file(file: Dict[str, Any], download_dir: str) -> str:
    """
    Загружает файл по его параметрам и сохраняет в указанную директорию.

    Args:
        file (Dict[str, Any]): Информация о файле для загрузки.
        download_dir (str): Путь к директории для сохранения файла.

    Returns:
        str: Имя загруженного файла.
    """
    time_begin = file["pt_time_begin"]
    file_name = file["pk_file_name"]
    url = f"https://fcnd.ru/api/getData/"
    params = {
        "datafile[time_begin]": time_begin,
        "datafile[file_name]": file_name
    }
    response = requests.get(url, params=params, stream=True)
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


def extract_file(file_name: str, download_dir: str) -> None:
    """
    Распаковывает архивный файл и удаляет его после успешной распаковки.

    Args:
        file_name (str): Имя файла для распаковки.
        download_dir (str): Путь к директории, где находится файл.
    """
    file_path = os.path.join(download_dir, file_name)
    if file_name.endswith(".zip"):
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            extracted_files = zip_file.namelist()
            if len(extracted_files) == 1:
                zip_file.extractall(download_dir)
            else:
                for extracted_file in extracted_files:
                    if re.search(r"\.\d{2}g", extracted_file):
                        zip_file.extract(extracted_file, download_dir)
                        break
        os.remove(file_path)
    elif file_name.endswith(".Z"):
        if platform.system().lower() == "windows":
            subprocess.run(
                f"\"C:\Program Files\Git\\bin\\bash.exe\" -c \"gunzip -f {get_unix_path(os.path.abspath(file_path))}\"",
                shell=True)
        else:
            subprocess.run(f"gunzip -f {file_path}", shell=True)


def extract_info_from_filename(filename: str) -> (str, int, int):
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

    station_name, year_full, day_of_year = extract_info_from_filename(input_file)
    year_suffix = str(year_full)[-2:]

    output_file_name = f"{station_name.lower()}{day_of_year:03d}0.{year_suffix}g"
    output_file = os.path.join(output_dir, output_file_name)

    command = [convbin_executable, input_file, "-r", "rinex3", "-n", output_file]

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


def main(dt_begin: str, dt_end: str, collection_name: str) -> None:
    """
    Основная функция, которая управляет процессом загрузки и распаковки файлов.

    Args:
        dt_begin (str): Начальная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
        dt_end (str): Конечная дата и время в формате 'DD-MM-YYYY hh:mm:ss'.
        collection_name (str): Имя коллекции для загрузки файлов.
    """
    download_dir = "./downloads"

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    try:
        collection_id = get_collection_id(collection_name)
        files_list = get_files_list(collection_id, dt_begin, dt_end)
        filtered_file_list = filter_files(files_list)
        unpacked_files = list()

        with concurrent.futures.ThreadPoolExecutor() as download_executor:
            future_to_file = {download_executor.submit(download_file, file, download_dir): file for file in
                              filtered_file_list}
            with concurrent.futures.ThreadPoolExecutor() as extract_executor:
                for future in concurrent.futures.as_completed(future_to_file):
                    archive_name = future.result()
                    extract_executor.submit(extract_file, archive_name, download_dir)
                    file_name = '.'.join(archive_name.split('.')[:-1])
                    # if file_name.endswith(".rnx"):
                    #     file_name = convert_rinex3_nav_to_rinex2(os.path.join(download_dir, file_name), "./merge_files")
                    unpacked_files.append(file_name)

        merger = RinexMerger(download_dir, './brdc')
        merger.merge_files('glo')

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
    parser.add_argument('--collection', type=str, dest='collection',
                        default='IAC_SDCM_gnss_data_daily_30sec',
                        help="the collection on the FSCD from which the files will be assembled")

    args = parser.parse_args()

    main(args.dt_begin, args.dt_end, args.collection)
