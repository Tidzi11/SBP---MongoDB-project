import os
import pandas as pd
from utils import connect_to_mongodb

RAW_DATA_PATH = "../data/raw/"
BATCH_SIZE = 10000

files = {
    "us_flights_2023" : ("us_civil_flights", "us-flights-2023.csv"),
    "cancelled_deverted_2023" : ("us_civil_flights", "cancelled-diverted-2023.csv"),
    "weather_meteo_by_airport" : ("us_civil_flights", "weather-meteo-by-airport.csv"),
    "airports_geolocation" : ("us_civil_flights", "airports-geolocation.csv"),
    "airports" :  ("airports", "airports.csv"),
    "airport_frequencies" : ("airports", "airport-frequencies.csv"),
    "runways" : ("airports", "runways.csv")
}

def load_data_from_csv(file_path, collection_name, database, batch_size = None):
    """Load CSV file into MongoDB collection"""
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return
        
        print(f"Loading data from {os.path.basename(file_path)}")

        if batch_size:
            df = pd.read_csv(file_path, nrows = batch_size)
        else:
            df = pd.read_csv(file_path)

        # Replace NaN with None for MongoDB compatibility
        df = df.where(pd.notnull(df), None) 

        # Convert DataFrame to list of dictionary records
        records = df.to_dict("records")

        collection = database[collection_name]
        result = collection.insert_many(records)

        print(f"Inserted {len(result.inserted_ids)} records into collection '{collection_name}'")

        return True
    except Exception as e:
        print(f"Error while loading file {file_path}: {e}")
        return False

def load_data_to_mongodb(database):
    """Load data from CSV files to MongoDB (in batches if specified)."""

    for collection_name, (subfolder, file_name) in files.items():
        file_path = os.path.join(RAW_DATA_PATH, subfolder, file_name)
        file_path = os.path.normpath(file_path)

        load_data_from_csv(file_path, collection_name, database, batch_size = BATCH_SIZE)

    print("Data loading completed.")
