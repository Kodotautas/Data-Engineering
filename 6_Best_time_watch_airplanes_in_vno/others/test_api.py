from FlightRadar24 import FlightRadar24API

fr_api = FlightRadar24API()

# vilnius - EIVY

flights = fr_api.get_flights()
airport_details = fr_api.get_airport_details("EYVI")

with open('airport_details.txt', 'w') as f:
    f.write(str(airport_details))


# airline_icao = "VNO"

# # You may also set a custom region, such as: bounds = "73,-12,-156,38"
# zone = fr_api.get_zones()["northamerica"]
# bounds = fr_api.get_bounds(zone)

# emirates_flights = fr_api.get_flights(
#     airline = airline_icao,
#     bounds = bounds
# )

# print(emirates_flights)