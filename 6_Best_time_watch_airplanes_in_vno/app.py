import pandas as pd
import json
from flask import Flask, render_template
from helpers.flights_data import FlightData
import datetime

app = Flask(__name__)

flight_data = FlightData('EYVI')

@app.route('/')
def best_time():
    # Get flights and weather data and filter by today and tomorrow
    flights = flight_data.group_flights_by_final_time()
    flights = flights[(flights['Datetime'].dt.date == pd.Timestamp.today().date()) | (flights['Datetime'].dt.date == pd.Timestamp.today().date() + pd.Timedelta(days=1))]
    weather_data = flight_data.get_weather()

    # Prepare data for Chart.js
    labels = flights.sort_values('Datetime', ascending=True)['Datetime'].tolist()
    labels = [datetime.datetime.strftime(x, '%Y-%m-%d %H:%M') for x in labels]
    data = flights.sort_values('Datetime', ascending=True)['Flights count'].tolist()

    flights_df = flight_data.concat_arrivals_departures()
    weather_data = flight_data.get_weather()

    # rename columns
    flights_df.rename(columns={'flight.status.generic.status.type': 'Type', 
                               'flight.identification.number.default': 'Flight number',
                               'flight.aircraft.model.text': 'Aircraft model', 
                               'flight.aircraft.country.name': 'Aircraft country', 
                               'flight.airline.short': 'Airline', 
                               'flight.airport.origin.position.region.city': 'Origin city', 
                               'flight.airport.destination.position.region.city': 'Destination city', 
                               'final_time': 'Final time',
                               'flight.status.generic.status.color': 'Status'}, inplace=True)

    flights_df['Final time'] = flights_df['Final time'].dt.strftime('%Y-%m-%d %H:%M')

    flights_data = flights_df.to_dict('records')
    flights_data_json = json.dumps(flights_data)

    # Pass data to template
    return render_template('best_time.html', labels=labels, data=data, weather_data=weather_data, flights_data=flights_data_json)


@app.route('/vno_flights_data')
def flights_data():
    # Get flights and weather data and filter by today and tomorrow
    flights_df = flight_data.concat_arrivals_departures()
    weather_data = flight_data.get_weather()

    # rename columns
    flights_df.rename(columns={'flight.status.generic.status.type': 'Type', 
                               'flight.identification.number.default': 'Flight number',
                               'flight.aircraft.model.text': 'Aircraft model', 
                               'flight.aircraft.country.name': 'Aircraft country', 
                               'flight.airline.short': 'Airline', 
                               'flight.airport.origin.position.region.city': 'Origin city', 
                               'flight.airport.destination.position.region.city': 'Destination city', 
                               'final_time': 'Final time',
                               'flight.status.generic.status.color': 'Status'}, inplace=True)

    flights_data = flights_df.to_dict('records')

    return render_template('flights_data.html', weather_data=weather_data, flights_data=flights_data, index=False)

if __name__ == '__main__':
    app.run(debug=True)