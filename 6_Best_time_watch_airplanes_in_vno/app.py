import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import plot

from flask import Flask, render_template
from helpers.flights_data import FlightData

app = Flask(__name__)

# Get flights data
flights = FlightData('EYVI').group_flights_by_final_time()

@app.route('/')
def flights_table():
    # Create a table of flight destinations
    fig = go.Figure(data=[go.Table(
        header=dict(values=['Final Time', 'Count']),
        cells=dict(values=[flights['final_time'], flights['count']])
    )])
    graph = plot(fig, output_type='div')

    return render_template('flight_info.html', flights=flights, graph=graph)

if __name__ == '__main__':
    app.run(debug=True)