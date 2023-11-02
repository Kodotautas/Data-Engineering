from FlightRadar24 import FlightRadar24API
import pandas as pd
import json

fr_api = FlightRadar24API()

airport_details = fr_api.get_airport_details("EYVI")

# Access the data
timestamp = airport_details['airport']['pluginData']['schedule']['arrivals']['timestamp']
humidity = airport_details['airport']['pluginData']['weather']['humidity']
sky_condition = airport_details['airport']['pluginData']['weather']['sky']['condition']
wind_direction = airport_details['airport']['pluginData']['weather']['wind']['direction']
wind_speed = airport_details['airport']['pluginData']['weather']['wind']['speed']['kmh']
temperature = airport_details['airport']['pluginData']['weather']['temp']['celsius']

arrivals = airport_details['airport']['pluginData']['schedule']['arrivals']['data']
departures = airport_details['airport']['pluginData']['schedule']['departures']['data']

# Create separate DataFrames for arrivals and departures
arrivals_df = pd.json_normalize(arrivals)
departures_df = pd.json_normalize(departures)

# Add a common key for merging
arrivals_df['key'] = 0
departures_df['key'] = 0

# Merge the DataFrames
flights_df = pd.merge(arrivals_df, departures_df, on='key', suffixes=('_arrival', '_departure'))

# Drop the key column
flights_df.drop('key', axis=1, inplace=True)
flights_df.to_csv('flights_data.csv', index=False)