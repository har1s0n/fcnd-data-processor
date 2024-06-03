import requests
import argparse
import datetime
import os
import re


# Функция для получения ID коллекции
def get_collection_id(collection_name):
    response = requests.get("https://fcnd.ru/api/getFilter/")
    response.raise_for_status()
    data = response.json()
    fields = data['answer']['t_meta_collection']['Fields']
    for item in fields:
        if item['c_short_name'] == collection_name:
            return item.get("pk_id")
    raise ValueError(f"Collection '{collection_name}' not found.")


# Функция для получения списка файлов за определенный период
def get_files_list(collection_id, time_begin, time_end):
    url = "https://fcnd.ru/api/getData/"
    params = {
        "filter[time_begin]": time_begin,
        "filter[time_end]": time_end,
        "filter[meta_collection][]": collection_id
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


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


def main():
    collection_name = "IAC_SDCM_gnss_data_daily_30sec"
    time_begin = "15-02-2024 00:00:00"
    time_end = "16-02-2024 23:59:59"
    download_dir = "./downloads"

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    try:
        collection_id = get_collection_id(collection_name)
        files_list = get_files_list(collection_id, time_begin, time_end)

        for file in files_list:
            time_begin = file["pt_time_begin"]
            file_name = file["pk_file_name"]
            download_file(time_begin, file_name, download_dir)

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
        collection = args.collection
    main()
