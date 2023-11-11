import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import plot

from flask import Flask, render_template
from helpers.flights_data import FlightData

app = Flask(__name__)

# Get flights and weather data and filter by today and tomorrow
flights = FlightData('EYVI').group_flights_by_final_time()
flights = flights[(flights['Datetime'].dt.date == pd.Timestamp.today().date()) | (flights['Datetime'].dt.date == pd.Timestamp.today().date() + pd.Timedelta(days=1))]

# Get concatenated arrivals and departures dataframe
flights_df = FlightData('EYVI').concat_arrivals_departures()
weather_data = FlightData('EYVI').get_weather()

@app.route('/')
def best_time():
    # Prepare data for Chart.js
    labels = flights.sort_values('Datetime', ascending=True)['Datetime'].tolist()
    data = flights.sort_values('Datetime', ascending=True)['Flights count'].tolist()

    # Pass data to template
    return render_template('best_time.html', labels=labels, data=data, weather_data=weather_data)

@app.route('/flights_data')
def flights_data():
    # Prepare data for html table
    flights_df = FlightData('EYVI').concat_arrivals_departures()
    # rename columns
    flights_df = flights_df.rename(columns={'flight.status.generic.status.type': 'Type', 
                                            'flight.identification.number.default': 'Flight number', 
                                            'flight.identification.callsign': 'Callsign', 
                                            'flight.aircraft.model.text': 'Aircraft model', 
                                            'flight.aircraft.country.name': 'Aircraft country', 
                                            'flight.airline.short': 'Airline', 
                                            'flight.airport.origin.position.region.city': 'Origin city', 
                                            'flight.airport.destination.position.region.city': 'Destination city', 
                                            'final_time': 'Final time'})

    flights_data = flights_df.to_dict('records')

    return render_template('flights_data.html', weather_data=weather_data, flights_data=flights_data, index=False)

if __name__ == '__main__':
    app.run(debug=True)