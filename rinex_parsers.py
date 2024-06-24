import pandas as pd
import os
from abc import ABC, abstractmethod
import re
from datetime import datetime, timezone


def remove_extra_spaces(s: str) -> str:
    """
    Удаляет из строки последовательности пробелов (более одного подряд), заменяя их одним пробелом.

    Args:
        s (str): Исходная строка.

    Returns:
        str: Строка с удаленными последовательностями пробелов.
    """
    return re.sub(r'\s{2,}', ' ', s)


def replace_D_to_E(string):
    return re.sub(r'(\d)D(\+|-)', r'\1E\2', string)


class BaseRinexParser(ABC):
    """
    BaseRinexParser - абстрактный базовый класс для парсинга rinex-файлов
    3.xx версий с навигационными данными  для ГНСС  GPS, GLONASS, Galileo, Beidou
    """

    @abstractmethod
    def parse_header(self, filepath):
        """
        Метод парсит заголовок rinex-файла
        :param filepath: путь к файлу для парсинга
        :return: данные из заголовка в виде pandas.DataFrame
        """
        pass

    @abstractmethod
    def parse_sv_data(self, filepath):
        """
        Метод парсит данные по спутникам

        :param filepath: путь к файлу для парсинга
        :return: данные с эфемеридами по спутникам в виде pandas.DataFrame
        """
        pass

    @abstractmethod
    def write_to_rinex_file(self, output_files_dir, header_data_frame, sv_data_frame):
        """
        Метод создает и записыавет ринекс файл на основе данных заголовка
        и спутниковых данных

        :param header_data_frame: DataFrame с данными заголовка
        :param sv_data_frame: DataFrame с данными по спутникам
        """
        pass

    @abstractmethod
    def get_columns_subset(self):
        """
        Метод возвращает список информативных колонок для заголовка

        :return: данные с эфемеридами по спутникам в виде pandas.DataFrame
        """
        pass


class GLONASSRinexParser(BaseRinexParser):
    """
    GLONASSRinexParser - класс для обработки rinex-файлов для GLONASS
    """

    def parse_header(self, filepath):
        """ GLONASS """
        header_info = {}
        with open(filepath, 'r') as file:
            for line in file:
                if "END OF HEADER" in line:
                    break

                if "RINEX VERSION / TYPE" in line:
                    header_info["rinex_ver"] = line[:20].strip()
                    continue

                if "PGM / RUN BY / DATE" in line:
                    if "UTC" in line:
                        try:
                            header_info["datetime_utc"] = datetime.strptime(line[40:55].strip(), '%Y%m%d %H%M%S')
                        except ValueError:
                            header_info["datetime_utc"] = datetime.strptime(line[40:56].strip(), '%Y%m%d %H%M%S')
                        continue
                    else:
                        try:
                            test = line[40:55].strip()
                            header_info["datetime_utc"] = datetime.strptime(line[40:55].strip(), '%d-%b-%y %H:%M')
                        except ValueError:
                            try:
                                header_info["datetime_utc"] = datetime.strptime(line[40:56].strip(), '%Y%m%d %H%M%S')
                            except ValueError:
                                header_info["datetime_utc"] = datetime.strptime(line[32:45].strip(), '%d-%b-%y %H:%M')
                        continue

                if ("TIME SYSTEM CORR" in line) and ("GLUT" in line):
                    line = replace_D_to_E(line)
                    header_info["GLUT"] = [float(line[5:22].strip()), float(line[22:38].strip()),
                                           int(line[39:45].strip()), int(line[46:50].strip())]
                    continue

                if "LEAP SECONDS" in line:
                    header_info["leap_sec"] = int(line[4:7].strip())
                    continue

        df = pd.DataFrame(
            {key: [value] if isinstance(value, list) else [value] for key, value in header_info.items()})
        return df

    def parse_sv_data(self, filepath):
        """ GLONASS """
        satellites_data = []

        with open(filepath, 'r') as file:
            # Пропускаем строки заголовка
            for line in file:
                if "END OF HEADER" in line:
                    break

            while True:
                sat_data = {}

                # Чтение первой строки
                line = replace_D_to_E(file.readline())
                if not line:  # конец файла
                    break

                # Парсим по жестко заданному формату
                sat_data["SV_label"] = line[0:3].strip()
                sat_data["SV"] = int(line[1:3].strip())
                sat_data["YYYY"] = int(line[4:8].strip())
                sat_data["MM"] = int(line[9:11].strip())
                sat_data["DD"] = int(line[12:14].strip())
                sat_data["hh"] = int(line[15:17].strip())
                sat_data["mm"] = int(line[18:20].strip())
                sat_data["ss"] = int(line[21:23].strip())
                sat_data["datetime_utc"] = datetime(sat_data["YYYY"], sat_data["MM"], sat_data["DD"], sat_data["hh"],
                                                    sat_data["mm"], sat_data["ss"])
                sat_data["FloatList"] = [float(line[23:42].strip()), float(line[42:61].strip()),
                                         float(line[61:80].strip())]

                # Чтение строк 2-4
                for i in range(2, 5):
                    line = replace_D_to_E(file.readline())
                    line = line.replace("D", "E")
                    for j in range(4):
                        start_idx = 4 + j * 19
                        end_idx = start_idx + 19
                        sat_data["FloatList"].append(float(line[start_idx:end_idx].strip()))

                satellites_data.append(sat_data)

        return pd.DataFrame(satellites_data)

    def write_to_rinex_file(self, output_files_dir, header_data_frame, sv_data_frame):
        """ GLONASS """
        folder = os.path.join(output_files_dir, "glo")
        os.makedirs(folder, exist_ok=True)

        t_now = datetime.now(timezone.utc)
        filename = f"GNSS00CMB_U_{t_now.year:04d}{t_now.timetuple().tm_yday:03d}{t_now.hour:02d}{t_now.minute:02d}_15M_RN.rnx"
        filename = os.path.join(folder, filename)

        with open(filename, 'w') as file:
            # WRITE HEADER INFO
            line = f"     3.04           N: GNSS NAV DATA    R: GLONASS          RINEX VERSION / TYPE"
            file.write(line + "\n")

            tmp = header_data_frame.head(1)['datetime_utc'].iloc[0].strftime('%Y%m%d %H%M%S')
            line = f"{'GNSS COMBINER':<40s}{tmp:>15s} UTC PGM / RUN BY / DATE"
            file.write(line + "\n")

            # TODO: add GLUT
            if "GLUT" in header_data_frame.columns:
                tmp_list = header_data_frame.head(1)['GLUT'].iloc[0]
                # line = f"GLUT{tmp_list[0]:>17.10E}{tmp_list[1]:>16.9E} {tmp_list[2]:>6d} {tmp_list[3]:>4d} {tmp_list[4]:>5s} {tmp_list[5]:>2d} TIME SYSTEM CORR   "
                line = f"GLUT {tmp_list[0]:17.10E} {tmp_list[1]:16.9E} {tmp_list[2]:6d} {tmp_list[3]:4d} TIME SYSTEM CORR"
                file.write(line + "\n")

            if "leap_sec" in header_data_frame.columns:
                tmp = int(header_data_frame.head(1)['leap_sec'].iloc[0])
                line = f"{tmp:>6d}                                                      LEAP SECONDS       "
                file.write(line + "\n")

            line = f"                                                            END OF HEADER      "
            file.write(line + "\n")

            # WRITE SV DATA
            # sv_data_frame.iloc[r, c]
            for row in sv_data_frame.itertuples():
                # Write SV / EPOCH / SV CLK
                line = f"{row.SV_label} {row.YYYY:>04d} {row.MM:>02d} {row.DD:>02d} {row.hh:>02d} {row.mm:>02d} {row.ss:>02d}{row.FloatList[0]:19.12E}{row.FloatList[1]:19.12E}{row.FloatList[2]:19.12E}"
                file.write(line + "\n")

                # Write BROADCAST ORBITS (1-3)
                for orb in range(3):
                    line = f"    {row.FloatList[orb * 4 + 3]:19.12E}{row.FloatList[orb * 4 + 4]:19.12E}{row.FloatList[orb * 4 + 5]:19.12E}{row.FloatList[orb * 4 + 6]:19.12E}"
                    file.write(line + "\n")

        return filename

    def get_columns_subset(self):
        """ GLONASS """
        return ['GLUT', 'leap_sec']
