import pandas as pd
import os
import datetime
from rinex_parsers import GLONASSRinexParser
import logging.config
import rinex_quality_check
from typing import Any, Optional

logger = logging.getLogger("rinex_merger")


class RinexMerger:
    """
    RinexMerger - класс, который объединяет навигационные данные для набора rinex-файлов.
    В результате работы метода merge_files(gnss) создается новый rinex-файл с
    обобщенными данными со всех файлов для выбранной ГНСС.
    """

    def __init__(self, input_files_dir, output_files_dir):
        self.input_files_dir = input_files_dir
        self.output_files_dir = output_files_dir
        self.parsers = {
            "glo": GLONASSRinexParser
        }

    def merge_files(self, gnss_type: str, start_date: Optional[datetime.datetime] = None,
                    end_date: Optional[datetime.datetime] = None) -> pd.DataFrame:
        """
        Объединяет файлы Rinex для указанного типа GNSS.

        Args:
            gnss_type (str): Тип GNSS (например, 'gps', 'glo', 'gal', 'bds').
            start_date (Optional[datetime.datetime]): Начальная дата для фильтрации данных по дате.
            end_date (Optional[datetime.datetime]): Конечная дата для фильтрации данных по дате.

        Returns:
            pd.DataFrame: DataFrame с объединенными спутниковыми данными.
        """
        gnss_abbreviature = self.get_gnss_abbreviature(gnss_type)
        parser = self.parsers[gnss_type]()

        gnss_files = self.get_gnss_files(gnss_abbreviature)

        result_df_sv_data, result_df_header_data = self.parse_files(gnss_files, parser, start_date, end_date)

        result_df_header_data = self.process_header_data(result_df_header_data, parser)
        result_df_sv_data = self.process_sv_data(result_df_sv_data)

        self.back_to_rinex_file(gnss_type, result_df_header_data.head(1), result_df_sv_data)

        return result_df_sv_data

    def get_gnss_abbreviature(self, gnss_type: str) -> str:
        """
        Возвращает аббревиатуру для указанного типа GNSS.

        Args:
            gnss_type (str): Тип GNSS (например, 'gps', 'glo', 'gal', 'bds').

        Returns:
            str: Аббревиатура GNSS.
        """
        gnss_abbreviature_map = {
            'gps': 'GN',
            'glo': 'RN',
            'gal': 'EN',
            'bds': 'CN'
        }
        return gnss_abbreviature_map[gnss_type]

    def get_gnss_files(self, gnss_abbreviature: str) -> list:
        """
        Получает список файлов GNSS для указанного типа GNSS.

        Args:
            gnss_abbreviature (str): Аббревиатура GNSS.

        Returns:
            list: Список файлов GNSS.
        """
        all_files = [f for f in os.listdir(self.input_files_dir) if
                     os.path.isfile(os.path.join(self.input_files_dir, f))]
        return [f for f in all_files if gnss_abbreviature.lower() + ".rnx" in f.lower()]

    def validate_rinex_file(self, filepath: str) -> bool:
        """
        Валидирует файл Rinex.

        Args:
            filepath (str): Путь к файлу Rinex.

        Returns:
            bool: True, если файл валидный, иначе False.
        """
        valid_status, valid_errors = rinex_quality_check.is_valid_rinex(filepath)
        rinex_quality_check.log_results(filepath, valid_status, valid_errors)
        return valid_status

    def parse_files(self, gnss_files: list, parser: Any, start_date: Optional[datetime.datetime],
                    end_date: Optional[datetime.datetime]) -> (pd.DataFrame, pd.DataFrame):
        """
        Парсит файлы GNSS и возвращает DataFrame с данными заголовка и спутников.

        Args:
            gnss_files (list): Список файлов GNSS.
            parser (Any): Парсер для файлов GNSS.
            start_date (Optional[datetime.datetime]): Начальная дата для фильтрации данных.
            end_date (Optional[datetime.datetime]): Конечная дата для фильтрации данных.

        Returns:
            (pd.DataFrame, pd.DataFrame): DataFrame с данными заголовка и DataFrame с данными спутников.
        """
        result_df_sv_data = pd.DataFrame()
        result_df_header_data = pd.DataFrame()

        for file in gnss_files:
            filepath = os.path.join(self.input_files_dir, file)

            if not self.validate_rinex_file(filepath):
                continue

            df_header, df_sv_data = self.parse_rinex_file(filepath, parser)

            if df_header is not None:
                result_df_header_data = pd.concat([result_df_header_data, df_header], ignore_index=True)

            if df_sv_data is not None:
                if start_date and end_date:
                    df_sv_data = self.filter_by_date(df_sv_data, start_date, end_date)
                result_df_sv_data = pd.concat([result_df_sv_data, df_sv_data], ignore_index=True)

        return result_df_sv_data, result_df_header_data

    def parse_rinex_file(self, filepath: str, parser: Any) -> (Optional[pd.DataFrame], Optional[pd.DataFrame]):
        """
        Парсит файл Rinex и возвращает данные заголовка и спутников.

        Args:
            filepath (str): Путь к файлу Rinex.
            parser (Any): Парсер для файла Rinex.

        Returns:
            (Optional[pd.DataFrame], Optional[pd.DataFrame]): DataFrame с данными заголовка и DataFrame с данными спутников.
        """
        df_header = parser.parse_header(filepath)

        if float(df_header.loc[0, 'rinex_ver']) >= 4 or float(df_header.loc[0, 'rinex_ver']) < 3:
            return None, None

        df_sv_data = parser.parse_sv_data(filepath)
        return df_header, df_sv_data

    def filter_by_date(self, df: pd.DataFrame, start_date: datetime.datetime,
                       end_date: datetime.datetime) -> pd.DataFrame:
        """
        Фильтрует DataFrame по дате.

        Args:
            df (pd.DataFrame): DataFrame с данными спутников.
            start_date (datetime.datetime): Начальная дата.
            end_date (datetime.datetime): Конечная дата.

        Returns:
            pd.DataFrame: Отфильтрованный DataFrame.
        """
        return df[(df['datetime_utc'] >= start_date) & (df['datetime_utc'] <= end_date)]

    def process_header_data(self, df: pd.DataFrame, parser: Any) -> pd.DataFrame:
        """
        Обрабатывает данные заголовка.

        Args:
            df (pd.DataFrame): DataFrame с данными заголовка.
            parser (Any): Парсер для файла Rinex.

        Returns:
            pd.DataFrame: Обработанный DataFrame.
        """
        sub_columns = parser.get_columns_subset()
        if set(sub_columns).issubset(df.columns):
            df.dropna(subset=sub_columns, how='all', inplace=True)

        df = df.sort_values(by='datetime_utc', ascending=False)
        df = df.bfill().ffill().reset_index(drop=True)
        return df

    def process_sv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрабатывает данные спутников.

        Args:
            df (pd.DataFrame): DataFrame с данными спутников.

        Returns:
            pd.DataFrame: Обработанный DataFrame.
        """
        df.drop_duplicates(subset=['SV', 'YYYY', 'MM', 'DD', 'hh', 'mm', 'ss'], inplace=True)
        df.sort_values(["SV", "datetime_utc"], ascending=[True, True], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def back_to_rinex_file(self, gnss_type: str, header_data_frame: pd.DataFrame, sv_data_frame: pd.DataFrame) -> None:
        """
        Записывает данные обратно в файл RINEX.

        Args:
            gnss_type (str): Тип GNSS.
            header_data_frame (pd.DataFrame): DataFrame с данными заголовка.
            sv_data_frame (pd.DataFrame): DataFrame с данными спутников.
        """
        parser = self.parsers[gnss_type]()
        output_name = parser.write_to_rinex_file(self.output_files_dir, header_data_frame, sv_data_frame)
        logger.info(f"Merged RINEX file for {gnss_type.upper()} is ready: {output_name}")
