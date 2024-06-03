import gzip
import zipfile
import requests
import argparse
import datetime
import os
import re


def get_collection_id():
    """"
    Функция для получения ID коллекции
    """
    response = requests.get("https://fcnd.ru/api/getFilter/")
    response.raise_for_status()
    data = response.json()
    fields = data['answer']['t_meta_collection']['Fields']
    for item in fields:
        if item['c_short_name'] == collection_name:
            return item.get("pk_id")
    raise ValueError(f"Collection '{collection_name}' not found.")


def get_files_list(collection_id):
    """"
    Функция для получения списка файлов за определенный период
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


def filter_files(files_list):
    """
    Функция для фильтрации списка файлов по заданной маске
    """
    filtered_files = []
    for file in files_list:
        file_name = file["pk_file_name"]
        if re.search(r"\.\d{2}g\.Z$", file_name) or \
                re.search(r"RN\.rnx", file_name) or \
                (file_name.endswith(".zip") and not re.search(r"\.rnx", file_name)):
            filtered_files.append(file)

    return filtered_files


def download_file(time_begin, file_name, download_dir):
    """"
    Функция для скачивания файла
    """
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


def extract_file(file_name, download_dir):
    """
    Функция для распаковки файла
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
    elif file_name.endswith(".Z"):
        with gzip.open(file_path, 'rb') as gz_file:
            extracted_content = gz_file.read()
            extracted_file_name = os.path.splitext(file_name)[0]
            extracted_file_path = os.path.join(download_dir, extracted_file_name)
            with open(extracted_file_path, 'wb') as extracted_file:
                extracted_file.write(extracted_content)


def main():
    download_dir = "./downloads"

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    try:
        collection_id = get_collection_id()
        files_list = get_files_list(collection_id)
        filtered_file_list = filter_files(files_list)

        for file in filtered_file_list:
            time_begin = file["pt_time_begin"]
            file_name = file["pk_file_name"]
            download_file(time_begin, file_name, download_dir)
            extract_file(file_name, download_dir)

    except Exception as e:
        print(f"An error occurred: {e}")


def validate_datetime(datetime_str):
    """
    Проверяет, соответствует ли введенная дата и время формату "DD-MM-YYYY hh:mm:ss".
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
    dt_begin = args.dt_begin
    dt_end = args.dt_end
    collection_name = args.collection

    main()
