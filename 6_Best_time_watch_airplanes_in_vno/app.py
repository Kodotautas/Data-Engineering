from flask import Flask, render_template

app = Flask(__name__)

# Simulated flight data
flights = [
    {'flight_number': 'F123', 'origin': 'New York', 'destination': 'Los Angeles'},
    {'flight_number': 'F456', 'origin': 'Chicago', 'destination': 'Miami'},
    {'flight_number': 'F789', 'origin': 'San Francisco', 'destination': 'Seattle'}
]

@app.route('/')
def show_flights():
    return render_template('flight_info.html', flights=flights)

if __name__ == '__main__':
    app.run(debug=True)