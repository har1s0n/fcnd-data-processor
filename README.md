# FCND Data Processor

## Overview

`FCND Data Processor` is a Python-based tool designed for processing and merging RINEX (Receiver Independent Exchange Format) nav files, specifically focusing on navigation data. It includes features such as downloading, validating, parsing, merging, and converting RINEX nav files, making it suitable for handling GNSS (Global Navigation Satellite System) data.

## Features

- **Download and Extraction**: Automates the downloading and extraction of GNSS data files.
- **Validation**: Checks the validity of RINEX files to ensure data integrity.
- **Parsing**: Parses the headers and satellite data from RINEX files.
- **Merging**: Merges multiple RINEX files into a single consolidated file.
- **Conversion**: Converts RINEX 2 files to RINEX 3 format.
- **Filtering**: Filters navigation data to include only specific epochs.
- **Analysing**: Analyzes the received dataframe and plots missing navigation measurements.

## Installation

Install the virtual environment in the root directory of the project:
```bash
python3 -m venv venv 
```

Activate the virtual environment

Unix/macOS:
```bash
source venv/bin/activate
```

Windows:
```bash
source venv/Scripts/activate
```

To install the required dependencies, use the following command:

```bash
pip install -r requirements.txt
```

## Usage

To run the main processing script, use the following command:
```bash
python main.py "DD-MM-YYYY hh:mm:ss" "DD-MM-YYYY hh:mm:ss"
```

Replace the placeholders with the desired start and end date-time in the format `DD-MM-YYYY hh:mm:ss`.

**Example:**
```bash
python main.py --dt_begin="10-06-2024 00:00:00" --dt_end="10-06-2024 23:59:59"
```

## Configuration

Configuration files should be placed in the `./config directory`. The configuration file should specify the collections of data to be processed.

## Logging

The tool logs its operations, including any validation errors, to help with debugging and ensuring data integrity.

## Contributing

1. Fork the repository.
2. Create a new branch (`git checkout -b feature-branch`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-branch`).
5. Create a new Pull Request.

## License

[MIT](https://choosealicense.com/licenses/mit/)
