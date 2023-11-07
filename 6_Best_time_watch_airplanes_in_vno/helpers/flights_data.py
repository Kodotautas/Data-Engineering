from FlightRadar24 import FlightRadar24API
import pandas as pd
import time

class FlightData:
    def __init__(self, airport_code):
        self.fr_api = FlightRadar24API()
        self.airport_details = self.fr_api.get_airport_details(airport_code)
        # self.timestamp = self.airport_details['airport']['pluginData']['schedule']['arrivals']['timestamp']
        # self.humidity = self.airport_details['airport']['pluginData']['weather']['humidity']
        # self.sky_condition = self.airport_details['airport']['pluginData']['weather']['sky']['condition']
        # self.wind_direction = self.airport_details['airport']['pluginData']['weather']['wind']['direction']
        # self.wind_speed = self.airport_details['airport']['pluginData']['weather']['wind']['speed']['kmh']
        # self.temperature = self.airport_details['airport']['pluginData']['weather']['temp']['celsius']
        
        self.arrivals = self.airport_details['airport']['pluginData']['schedule']['arrivals']['data']
        self.departures = self.airport_details['airport']['pluginData']['schedule']['departures']['data']
        
    def get_arrivals(self):
        """Returns arrivals dataframe"""
        arrivals_df = pd.json_normalize(self.arrivals)
        arrivals_df['arrival_time'] = pd.to_datetime(arrivals_df['flight.time.scheduled.arrival'] + arrivals_df['flight.airport.destination.timezone.offset'], unit='s')
        arrivals_df['arrival_estimated_time'] = pd.to_datetime(arrivals_df['flight.time.estimated.arrival'] + arrivals_df['flight.airport.destination.timezone.offset'], unit='s')
        arrivals_df['final_time'] = arrivals_df['arrival_estimated_time'].fillna(arrivals_df['arrival_time'])
        columns_to_leave = ['flight.status.generic.status.type', 'flight.identification.number.default', 'flight.identification.callsign', 'flight.aircraft.model.text', 'flight.aircraft.country.name', 'flight.airline.short', 'flight.airport.origin.position.region.city', 'final_time']
        return arrivals_df[columns_to_leave]

    def get_departures(self):
        """Returns departures dataframe"""
        departures_df = pd.json_normalize(self.departures)
        departures_df['departure_time'] = pd.to_datetime(departures_df['flight.time.scheduled.departure'] + departures_df['flight.airport.origin.timezone.offset'], unit='s')
        departures_df['departure_estimated_time'] = pd.to_datetime(departures_df['flight.time.estimated.departure'] + departures_df['flight.airport.origin.timezone.offset'], unit='s')
        departures_df['final_time'] = departures_df['departure_estimated_time'].fillna(departures_df['departure_time'])
        columns_to_leave = ['flight.status.generic.status.type', 'flight.identification.number.default', 'flight.identification.callsign', 'flight.aircraft.model.text', 'flight.aircraft.country.name', 'flight.airline.short', 'flight.airport.destination.position.region.city', 'final_time']
        return departures_df[columns_to_leave]
    
    def concat_arrivals_departures(self):
        """Returns arrivals and departures dataframe concatenated"""
        arrivals_df = self.get_arrivals()
        departures_df = self.get_departures()
        flights_df = pd.concat([arrivals_df, departures_df])
        flights_df = flights_df.sort_values(by='final_time', ascending=True)
        return flights_df
    
    def group_flights_by_final_time(self):
        """Returns count of flights by 15 minutes time interval also add count of arrivals and departures"""
        flights_df = self.concat_arrivals_departures()
        flights_df['Datetime'] = flights_df['final_time'].dt.round('15min')
        flights_df = flights_df.groupby('Datetime').size().reset_index(name='Flights count')
        return flights_df
    
def run():
    start_time = time.time()

    airport_code = 'EYVI'
    fd = FlightData(airport_code)
    flights = fd.group_flights_by_final_time()
    print(f'Flights in {airport_code} airport: {len(flights)}')

    print(f'Done in {time.time() - start_time} seconds')

if __name__ == '__main__':
    run()