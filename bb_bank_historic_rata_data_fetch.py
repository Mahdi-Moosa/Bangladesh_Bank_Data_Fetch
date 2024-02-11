import argparse
import os
import requests
import pandas as pd
import calendar
import random
import time
from datetime import datetime, timezone
import io  

DATA_FOLDER = 'bd_bank_historical_interest_rate_data'

def get_data_table(month, year):
    """
    Fetches data table for a specific month and year from a website.

    Args:
        month (int): The month for which data is to be fetched (1 to 12).
        year (int): The year for which data is to be fetched.

    Returns:
        pandas.DataFrame or None: The data table for the specified month and year if fetched successfully,
        else returns None.
    """
    # Convert month number to month name and capitalize first letter
    month_name = calendar.month_name[month].capitalize()

    # Find the last day of the month
    last_day = calendar.monthrange(year, month)[1]

    # Define the URL and payload (data) for the POST request
    url = 'https://www.bb.org.bd/en/index.php/financialactivity/interestdeposit'
    payload = {'select_month': month_name, 'select_year': str(year), 'bank_deposit_rate_submit': 'Search'}

    # Send the POST request
    response = requests.post(url, data=payload)

    # Check the response status code
    if response.status_code == 200:
        print(f"Data fetched for {month_name}, {year}")
        # Use StringIO to wrap the HTML content
        data_tables = pd.read_html(io.StringIO(response.text))
        data_table = data_tables[0]
        date_column = pd.to_datetime(f'{year}-{month:02}-{last_day}', format='%Y-%m-%d')
        data_table[('month_year', 'month_year')] = date_column
        return data_table
    else:
        print(f"Failed to fetch data for {month_name}, {year} with status code:", response.status_code)
        return None

def load_or_fetch_data(month, year):
    """
    Load data from saved Parquet file if exists, otherwise fetch from the website.

    Args:
        month (int): The month for which data is to be fetched (1 to 12).
        year (int): The year for which data is to be fetched.

    Returns:
        pandas.DataFrame: The data table for the specified month and year.
    """
    file_path = os.path.join(DATA_FOLDER, f"{month}_{year}.parquet")
    if os.path.exists(file_path):
        print(f"Loading data for {calendar.month_name[month]}, {year} from {file_path}")
        return pd.read_parquet(file_path)
    else:
        data_table = get_data_table(month, year)
        if data_table is not None:
            data_table.to_parquet(file_path, index=True)
        return data_table

def create_fetch_list(start_year, end_year):
    """
    Creates a list of month, year data to fetch.

    Args:
        start_year (int): The starting year of the range.
        end_year (int): The ending year of the range.

    Returns:
        list: List of tuples containing month and year combinations to fetch.
    """
    fetch_list = []

    # Check if end year is smaller than start year
    if end_year < start_year:
        raise ValueError("End year cannot be smaller than start year.")

    # Get the current month and year
    current_year = datetime.now(timezone.utc).year
    current_month = datetime.now(timezone.utc).month

    print("Current year:", current_year)
    print("Current month:", current_month)

    # Iterate over each year in the range
    for year in range(start_year, end_year + 1):
        # Iterate over all months in the year
        max_month = 12 if year < current_year else current_month  # Limit months to current month if year is current year
        print("Max month for year", year, ":", max_month)
        for month in range(1, max_month + 1):
            fetch_list.append((month, year))

    # Remove any month, year beyond the current month, year
    fetch_list = [(month, year) for month, year in fetch_list if (year < current_year) or (year == current_year and month <= current_month)]

    print("Fetch list:", fetch_list)
    
    return fetch_list

def get_data_for_months(fetch_list):
    """
    Fetches data tables for a list of month, year combinations.

    Args:
        fetch_list (list): List of tuples containing month and year combinations to fetch.

    Returns:
        pandas.DataFrame: Concatenated data table for all the months in the fetch list.
    """
    all_tables = []  # Initialize an empty list to store data tables
    consecutive_zero_rows = 0  # Counter for consecutive months with zero rows
    consecutive_zero_months = []  # List to store consecutive zero months

    # Iterate over each month, year combination in reverse order
    for month, year in reversed(fetch_list):
        data_table = load_or_fetch_data(month, year)
        print(f'The shape for fetched table {month}, {year} is {data_table.shape}')
        if data_table is not None:
            if len(data_table) > 0:  # Check if the size of the fetched table is greater than zero
                all_tables.append(data_table)
                consecutive_zero_rows = 0
                consecutive_zero_months = []
            else:
                consecutive_zero_rows += 1
                consecutive_zero_months.append(f"{calendar.month_name[month]}, {year}")
                if consecutive_zero_rows == 5:
                    print(f"No data for consecutive 5 months:", ", ".join(consecutive_zero_months))
                    print("Data fetch aborted.")
                    break  # Stop fetching data if there are consecutive zero rows for 5 months
                
    return pd.concat(all_tables, ignore_index=True)

def main():
    """
    Main function to fetch data, save it to a Parquet file, and print a message.
    """
    parser = argparse.ArgumentParser(description="Fetch bank historical interest rate data.")
    parser.add_argument("years", nargs=2, metavar=('start_year', 'end_year'), type=int, help="Start year and end year for data fetching")
    args = parser.parse_args()

    start_year, end_year = args.years

    # Create data folder if not exists
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    try:
        fetch_list = create_fetch_list(start_year, end_year)
    except ValueError as e:
        print(e)
        return

    if not fetch_list:
        print("No months to fetch data for.")
        return

    result_table = get_data_for_months(fetch_list)

    # Generate file name for the final combined Parquet file
    file_name_combined = 'historic_rate_data_bd_banks_combined.parquet'
    if os.path.exists(file_name_combined):
        current_time = datetime.now(timezone.utc).strftime("_%Y-%m-%d_%H-%M-%S_GMT")
        file_name_combined = file_name_combined.split('.parquet')[0] + current_time + '.parquet'

    # Save the final combined table as a Parquet file
    result_table.to_parquet(file_name_combined, index=True)
    print(f"Result table saved as {file_name_combined}")

if __name__ == "__main__":
    main()
