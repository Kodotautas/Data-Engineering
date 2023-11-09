import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import plot

from flask import Flask, render_template
from helpers.flights_data import FlightData

app = Flask(__name__)

# Get flights and weather data
flights = FlightData('EYVI').group_flights_by_final_time()
weather_data = FlightData('EYVI').get_weather()

@app.route('/')
def home():
    # Prepare data for Chart.js
    labels = flights.sort_values('Datetime', ascending=True)['Datetime'].tolist()
    data = flights.sort_values('Datetime', ascending=True)['Flights count'].tolist()

    # Pass data to template
    return render_template('best_time.html', labels=labels, data=data, weather_data=weather_data)

if __name__ == '__main__':
    app.run(debug=True)