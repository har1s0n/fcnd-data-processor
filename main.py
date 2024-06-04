import subprocess
import zipfile
import requests
import argparse
import datetime
import os
import re
import concurrent.futures


def get_collection_id(collection_name):
    response = requests.get("https://fcnd.ru/api/getFilter/")
    response.raise_for_status()
    data = response.json()
    fields = data['answer']['t_meta_collection']['Fields']
    for item in fields:
        if item['c_short_name'] == collection_name:
            return item.get("pk_id")
    raise ValueError(f"Collection '{collection_name}' not found.")


def get_files_list(collection_id, dt_begin, dt_end):
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
    filtered_files = []
    file_patterns = [
        r"\.\d{2}g\.Z$",
        r"RN\.rnx",
        lambda filename: filename.endswith(".zip") and not re.search(r"\.rnx", filename)
    ]

    for file in files_list:
        file_name = file["pk_file_name"]
        if any(re.search(pattern, file_name) if isinstance(pattern, str) else pattern(file_name) for pattern in
               file_patterns):
            filtered_files.append(file)

    return filtered_files


def download_file(file, download_dir):
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


def extract_file(file_name, download_dir):
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
        subprocess.run(f"gunzip -f {file_path}", shell=True)


def main(dt_begin, dt_end, collection_name):
    download_dir = "./downloads"

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    try:
        collection_id = get_collection_id(collection_name)
        files_list = get_files_list(collection_id, dt_begin, dt_end)
        filtered_file_list = filter_files(files_list)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file in filtered_file_list:
                executor.submit(download_file, file, download_dir)
                executor.submit(extract_file, file['pk_file_name'], download_dir)

    except Exception as e:
        print(f"An error occurred: {e}")


def validate_datetime(datetime_str):
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