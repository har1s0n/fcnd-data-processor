import pandas as pd
import os
from rinex_parsers import GLONASSRinexParser
import logging.config
import rinex_quality_check

logger = logging.getLogger("rinex_merger")


class RinexMerger:
    """
    RinexMerger - класс, который объединяет навигационные данные для нобора rinex-файлов.
    В результате работы метода merge_files(gnss) создается новый rinex-файл с
    обобщенными данными со всех файлов для выбранной ГНСС.
    """

    def __init__(self, input_files_dir, output_files_dir):
        self.input_files_dir = input_files_dir
        self.output_files_dir = output_files_dir
        self.parsers = {
            "glo": GLONASSRinexParser
        }

    def merge_files(self, gnss_type):
        gnss_abbreviature = {
            'gps': 'GN',
            'glo': 'RN',
            'gal': 'EN',
            'bds': 'CN'
        }
        # Определяем парсер для ГНСС
        parser = self.parsers[gnss_type]()

        all_files = [f for f in os.listdir(self.input_files_dir) if
                     os.path.isfile(os.path.join(self.input_files_dir, f))]
        gnss_files = [f for f in all_files if gnss_abbreviature[gnss_type].lower() + ".rnx" in f.lower()]

        # Создание результирующего DataFrame
        result_df_sv_data = pd.DataFrame()
        result_df_header_data = pd.DataFrame()

        # Парсинг файлов
        for file in gnss_files:
            # print("Reading file: " + file)
            # logger.debug()
            filepath = os.path.join(self.input_files_dir, file)

            # валидация
            valid_status, valid_errors = rinex_quality_check.is_valid_rinex(filepath)
            rinex_quality_check.log_results(filepath, valid_status, valid_errors)

            if not valid_status:
                continue

            # Парсинг заголовка ринекс-файла
            df = parser.parse_header(filepath)

            # Пока обработка ринекс фалов только 3 версии
            if float(df.loc[0, 'rinex_ver']) >= 4 or float(df.loc[0, 'rinex_ver']) < 3:
                continue

            result_df_header_data = pd.concat([result_df_header_data, df], ignore_index=True)

            # Парсинг спутниковых данных
            df = parser.parse_sv_data(filepath)
            result_df_sv_data = pd.concat([result_df_sv_data, df], ignore_index=True)

        # Обработка результирующего DataFrame с данными заголовков файлов
        # result_df_header_data.dropna(subset=['GPSA', 'GPSB', 'GPUT'], how='all', inplace=True)
        sub_columns = parser.get_columns_subset()
        if set(sub_columns).issubset(result_df_header_data.columns):
            result_df_header_data.dropna(subset=sub_columns, how='all', inplace=True)

        result_df_header_data = result_df_header_data.sort_values(by='datetime_utc', ascending=False)
        result_df_header_data = result_df_header_data.bfill()
        result_df_header_data = result_df_header_data.ffill()
        result_df_header_data.reset_index(drop=True, inplace=True)

        # Обработка результирующего DataFrame с данными по спутникам
        result_df_sv_data.drop_duplicates(subset=['SV', 'YYYY', 'MM', 'DD', 'hh', 'mm', 'ss'], inplace=True)
        result_df_sv_data.sort_values(["SV", "datetime_utc"], ascending=[True, True], inplace=True)
        result_df_sv_data.reset_index(drop=True, inplace=True)

        # Запись обобщенных данных обратно в RINEX
        self.back_to_rinex_file(gnss_type, result_df_header_data.head(1), result_df_sv_data)

        return result_df_sv_data

    def back_to_rinex_file(self, gnss_type, header_data_frame, sv_data_frame):
        parser = self.parsers[gnss_type]()
        output_name = parser.write_to_rinex_file(self.output_files_dir, header_data_frame, sv_data_frame)
        logger.info(f"Merged RINEX file for {gnss_type.upper()} is ready: {output_name} ")
