from FlightRadar24 import FlightRadar24API
import pandas as pd
import time


class FlightData:
    def __init__(self, airport_code):
        self.fr_api = FlightRadar24API()
        airport_details = self.fr_api.get_airport_details(airport_code)
        airport = airport_details['airport']
        plugin_data = airport['pluginData']
        weather = plugin_data['weather']
        schedule = plugin_data['schedule']

        self.humidity = weather['humidity']
        self.sky_condition = weather['sky']['condition']
        self.wind_direction = weather['wind']['direction']
        self.wind_speed = weather['wind']['speed']['kmh']
        self.temperature = weather['temp']['celsius']
        self.arrivals = self.process_flights(schedule['arrivals']['data'])
        self.departures = self.process_flights(schedule['departures']['data'])

    def process_flights(self, flights):
        df = pd.json_normalize(flights)
        df['final_time'] = pd.to_datetime(df['flight.time.estimated.arrival'].fillna(df['flight.time.scheduled.arrival']) + df['flight.airport.destination.timezone.offset'], unit='s')
        return df

    def get_arrivals(self):
        """Returns arrivals dataframe"""
        columns_to_leave = ['flight.status.generic.status.type', 'flight.identification.number.default', 'flight.identification.callsign', 'flight.aircraft.model.text', 'flight.aircraft.country.name', 'flight.airline.short', 'flight.airport.origin.position.region.city', 'final_time']
        return self.arrivals[columns_to_leave]

    def get_departures(self):
        """Returns departures dataframe"""
        columns_to_leave = ['flight.status.generic.status.type', 'flight.identification.number.default', 'flight.aircraft.model.text', 'flight.aircraft.country.name', 'flight.airline.short', 'flight.airport.destination.position.region.city', 'final_time']
        return self.departures[columns_to_leave]
    
    def concat_arrivals_departures(self):
        """Returns arrivals and departures dataframe concatenated"""
        flights_df = pd.concat([self.get_arrivals(), self.get_departures()])
        flights_df = flights_df.sort_values(by='final_time', ascending=True).fillna('')
        # Add Vilnius city to origin and destination
        flights_df.loc[flights_df['flight.status.generic.status.type'] == 'departure', 'flight.airport.origin.position.region.city'] = 'Vilnius'
        flights_df.loc[flights_df['flight.status.generic.status.type'] == 'arrival', 'flight.airport.destination.position.region.city'] = 'Vilnius'
        return flights_df
    
    def group_flights_by_final_time(self):
        """Returns count of flights by 15 minutes time interval also add count of arrivals and departures"""
        flights_df = self.concat_arrivals_departures()
        flights_df['Datetime'] = flights_df['final_time'].dt.round('15min')
        flights_df = flights_df.groupby('Datetime').size().reset_index(name='Flights count')
        return flights_df
    
    def get_weather(self):
        """Returns weather data"""
        humidity = self.humidity
        sky_condition = self.sky_condition['text']
        wind_direction = self.wind_direction['text']
        wind_speed = self.wind_speed
        temperature = self.temperature
        return f'Humidity: {humidity}% | Sky condition: {sky_condition} | Wind direction: {wind_direction} | Wind speed: {wind_speed} km/h | Temperature: {temperature} °C'

def run():
    start_time = time.time()

    airport_code = 'EYVI'
    fd = FlightData(airport_code)
    flights = fd.group_flights_by_final_time()
    print(f'Flights in {airport_code} airport: {len(flights)}')

    print(f'Done in {time.time() - start_time} seconds')

if __name__ == '__main__':
    run()