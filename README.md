# SARB Data Processing and Analytics

This project provides a pipeline for processing, analyzing, and reporting on financial data, specifically tailored for the South African Reserve Bank (SARB) BA900 return.

## Features

- **Data Loading**: Loads data from Parquet or CSV files.
- **Configurable Mapping**: Uses a JSON configuration to map raw data to meaningful fields.
- **Data Aggregation**: Calculates aggregate metrics based on configurable rules.
- **Time-Series Analytics**: Computes month-on-month, year-on-year, and market share analytics.
- **Reporting**: Generates a multi-sheet Excel report and a text summary of the analytics.
- **Modular & Efficient**: The code is refactored for better readability, performance, and maintainability.

## Project Structure

```
.sarb_api/
├── config/
│   ├── ba_900_mapping.json
│   └── config.json
├── data/
│   └── ba_900/
│       ├── combined_data.parquet
│       └── ... (other data and output files)
├── sarb_data_process.py
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup and Installation

1.  **Clone the repository (once on GitHub):**
    ```bash
    git clone <repository-url>
    cd sarb_api
    ```

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the data processing pipeline, execute the main script:

```bash
python sarb_data_process.py
```

The script will perform all the processing steps and generate the output files in the `data/ba_900` directory.

## Configuration

The behavior of the script is controlled by the `config/config.json` file. Here you can configure:

-   File paths and names.
-   Rules for loan aggregation.

The `config/ba_900_mapping.json` file defines how the raw data items are mapped to the fields used in the analysis.
