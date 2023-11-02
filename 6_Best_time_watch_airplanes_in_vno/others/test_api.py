from FlightRadar24 import FlightRadar24API


fr_api = FlightRadar24API()

airport_details = fr_api.get_airport_details("EYVI")

with open('EYVI_fig.txt', 'w') as f:
    f.write(str(airport_details))