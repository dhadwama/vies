 Viivi

## Description
Viivi is an automation script designed to verify the validity of VAT numbers issued by any Member State using the VIES API. The script reads VAT numbers from a Snowflake database, validates them via the VIES SOAP service, logs results and errors, and writes validated data back to Snowflake.

## Features
- Verify VAT numbers using the VIES API.
- Read VAT numbers from a Snowflake database.
- Log validation results and errors.
- Write validated data back to Snowflake.
- Retry validation for erroneous VAT numbers after a delay.

## Requirements
- Python 3.x
- `requests` library
- `pandas` library
- `numpy` library
- `python-dotenv` library
- `snowflake-connector-python` library

## Setup
1. Clone the repository.
2. Install the required Python libraries using pip:
    pip install -r requirements.txt

3. Set up your environment variables in a `.env` file:
    SF_ACCOUNT=<your_snowflake_account>
    SF_USER=<your_snowflake_user>
    SF_WAREHOUSE=<your_snowflake_warehouse>
    SF_DATABASE=<your_snowflake_database>
    SF_ROLE=<your_snowflake_role>
    SF_SCHEMA=<your_snowflake_schema>
    SF_PASSWORD=<your_snowflake_password>

## Usage
1. Run the script:
    python viivi.py

## Notes
- Ensure that your Snowflake credentials and environment variables are correctly set up in the `.env` file.
- The script will log validation results and errors, and retry validation for erroneous VAT numbers after a delay.
