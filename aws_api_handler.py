import os
import requests
import boto3
import pandas as pd
from datetime import datetime
from io import BytesIO


def get_s3_client():
    """
    Creates and returns an S3 client initialized with environment variables.

    Returns:
        boto3.Client: An S3 client object.
    """
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )


def fetch_api_data(start_time, end_time):
    """
    Fetches energy data from the SolarEdge API for a specified time range.

    Args:
        start_time (str): The start datetime for the data fetch (formatted as 'YYYY-MM-DD HH:MM:SS').
        end_time (str): The end datetime for the data fetch (formatted as 'YYYY-MM-DD HH:MM:SS').

    Returns:
        dict: A dictionary containing the energy data in JSON format, or None if the request fails.
    """
    site_id = os.getenv("SITE_ID")
    api_key = os.getenv("API_KEY")

    if not site_id or not api_key:
        raise ValueError("API_KEY and SITE_ID must be provided in the environment variables.")

    energy_endpoint = f"https://monitoringapi.solaredge.com/site/{site_id}/energyDetails"
    energy_api_parameters = {
        "api_key": api_key,
        "timeUnit": "QUARTER_OF_AN_HOUR",
        "startTime": start_time,
        "endTime": end_time,
    }

    try:
        response = requests.get(energy_endpoint, params=energy_api_parameters)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def download_file_from_s3(bucket_name, file_key):
    """
    Downloads a file from S3 and returns it as a Pandas DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) of the file in the S3 bucket.

    Returns:
        pd.DataFrame: A DataFrame containing the file's data, or an empty DataFrame if the download fails.
    """
    s3_client = get_s3_client()
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = s3_object['Body'].read()
        df = pd.read_parquet(BytesIO(file_content))
        return df
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return pd.DataFrame()


def upload_file_to_s3(bucket_name, file_key, df):
    """
    Uploads a DataFrame as a Parquet file to an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The key (path) to save the file in the S3 bucket.
        df (pd.DataFrame): The DataFrame to upload.

    Returns:
        None
    """
    s3_client = get_s3_client()
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=buffer)
        print(f"File successfully uploaded to S3 with key: {file_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def fetch_missing_data(start_date, end_date):
    """
    Fetches data for any specified missing date range and updates the current meter data file on S3.

    Args:
        start_date (str): Start date for missing data.
        end_date (str): End date for missing data.
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    file_key = os.getenv("FILE_KEY")

    # Download the current meter data from S3
    df = download_file_from_s3(bucket_name, file_key)

    # Ensure 'date' column is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Fetch new data from the API
    new_data_json = fetch_api_data(start_date, end_date)

    if new_data_json:
        new_data = new_data_json.get("energyDetails", {}).get("meters", [])
        rows = []

        # Extract the data from the API response
        for meter in new_data:
            meter_type = meter['type']
            for value in meter['values']:
                row = {
                    'date': value['date'],
                    'type': meter_type,
                    'value': value.get('value', 0)
                }
                rows.append(row)

        # Convert new data into a DataFrame
        new_data_df = pd.DataFrame(rows)

        # Ensure 'date' column in new data is in datetime format
        new_data_df['date'] = pd.to_datetime(new_data_df['date'])

        # Reshape the new data
        reshaped_df = new_data_df.pivot_table(index='date', columns='type', values='value', aggfunc='last')
        reshaped_df.reset_index(inplace=True)

        # Append the new data to the existing data, keeping only the unique dates
        df = pd.concat([df, reshaped_df]).drop_duplicates(subset='date', keep='last')

        # Upload the updated file back to S3
        upload_file_to_s3(bucket_name, file_key, df)
        print(f"Updated data successfully uploaded to S3 with key: {file_key}")
    else:
        print("No new data found for the specified date range.")


def update_meter_data():
    """
    Updates the meter_data.parquet file stored in S3 by appending the latest energy data
    from the SolarEdge API.

    Args:
        None

    Returns:
        None
    """
    bucket_name = os.getenv("S3_BUCKET_NAME")
    file_key = os.getenv("FILE_KEY")

    df = download_file_from_s3(bucket_name, file_key)
    df['date'] = pd.to_datetime(df['date'])

    if df.empty:
        start_time = datetime(2024, 11, 8).strftime('%Y-%m-%d %H:%M:%S')
    else:
        most_recent_datetime = df['date'].max()
        start_time = (pd.to_datetime(most_recent_datetime) + pd.Timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_data_json = fetch_api_data(start_time, end_time)

    if new_data_json:
        new_data = new_data_json.get("energyDetails", {}).get("meters", [])
        rows = []

        for meter in new_data:
            meter_type = meter['type']
            for value in meter['values']:
                row = {
                    'date': value['date'],
                    'type': meter_type,
                    'value': value.get('value', 0)
                }
                rows.append(row)

        new_data_df = pd.DataFrame(rows)
        new_data_df['date'] = pd.to_datetime(new_data_df['date'])
        reshaped_df = new_data_df.pivot_table(index='date', columns='type', values='value', aggfunc='last')
        reshaped_df.reset_index(inplace=True)

        if not df.empty:
            df = pd.concat([df, reshaped_df]).drop_duplicates(subset='date', keep='last')
        else:
            df = reshaped_df

        upload_file_to_s3(bucket_name, file_key, df)
    else:
        print("No new data fetched from the API.")
