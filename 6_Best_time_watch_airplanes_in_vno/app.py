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
    # Create a table of flight destinations
    fig = px.bar(flights.sort_values('Datetime', ascending=False), 
                 x='Flights count', y='Datetime',
                color='Flights count',
                orientation='h',
                color_continuous_scale=["darkgrey", "darkgreen"],
                )

    # Calculate the height of the table based on the number of rows
    row_height = 20  # Set the height of each row in pixels
    num_rows = len(flights['Datetime'])
    table_height = row_height * num_rows

    # Set the height of the table and disable scrolling
    fig.update_layout(height=table_height, margin=dict(l=0, r=0, t=5, b=0), template='plotly_dark')
    fig.update_xaxes(visible=False, showticklabels=False)
    fig.update_yaxes(visible=True, showticklabels=True, tickmode='array', tickvals=flights['Datetime'], 
                     ticktext=flights['Datetime'].dt.strftime('%m-%d %H:%M'), 
                     type='category'
                     )

    graph = plot(fig, output_type='div')

    return render_template('best_time.html', flights=flights, graph=graph, weather=weather_data)


if __name__ == '__main__':
    app.run(debug=True)