import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import plot
import numpy as np

from flask import Flask, render_template
from helpers.flights_data import FlightData
import colorlover as cl

app = Flask(__name__)

# Get flights data
flights = FlightData('EYVI').group_flights_by_final_time()

@app.route('/')
def flights_table():
    # Create a color scale
    color_scale = cl.scales['9']['seq']['Blues']

    # Normalize the 'Count' values to the range of the color scale
    normalized_counts = np.interp(flights['count'], (min(flights['count']), max(flights['count'])), (0, len(color_scale) - 1))

    # Map the normalized 'Count' values to the color scale
    colors = [color_scale[int(i)] for i in normalized_counts]

    # Create a table of flight destinations
    fig = go.Figure(data=[go.Table(
        header=dict(values=['Datetime', 'Planes count']),
        cells=dict(values=[flights['final_time'], flights['count']],
                   fill_color=[['white'], colors])
    )])

    # Calculate the height of the table based on the number of rows
    row_height = 30  # Set the height of each row in pixels
    num_rows = len(flights['final_time'])
    table_height = row_height * num_rows

    # Set the height of the table and disable scrolling
    fig.update_layout(height=table_height, margin=dict(l=30, r=30, t=30, b=30))
    fig.update_yaxes(visible=False, showticklabels=False)

    graph = plot(fig, output_type='div')

    return render_template('flight_info.html', flights=flights, graph=graph)

if __name__ == '__main__':
    app.run(debug=True)