import requests

url = "https://car-api2.p.rapidapi.com/api/years"

headers = {
	"X-RapidAPI-Key": "17d0e1118dmsh25ef42b8e87e67ap104889jsn2072acd0016f",
	"X-RapidAPI-Host": "car-api2.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

print(response.json())