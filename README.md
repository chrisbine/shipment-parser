# shipment-parser

This Python script downloads, validates, transforms, and stores operational capacity data from the Energy Transfer's website into a SQLite database.

## Features

- Downloads CSV data from Energy Transfer's operational capacity endpoint (https://twtransfer.energytransfer.com/ipost/TW/capacity/operationally-available)
- Validates data structure and content
- Transforms data to match database schema
- Stores data in a local SQLite database
- Processes data for the last N days (configurable)
- TODO: Validate needed columns and remove unused ones
- TODO: Add data validation to filter bad data / check data types
- TODO: Add option to overwrite/keep DB values if importing the same file
- TODO: Optimize performance / test file types


## Requirements

- Python 3.7+
- Required packages:
  ```bash
  pip install -r requirements.txt
