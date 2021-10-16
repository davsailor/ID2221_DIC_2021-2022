import json

path = "./../utils/italian_cities_extractor/cities.json"

with open(path, 'r') as f:
	full = json.load(f)
	for city in full:
		if city["country"] == "IT":
			print(str(city["id"]))
