import pandas as pd
import datetime as dt

CITIES = ['Milano', 'Torino', 'Genova', 'Aosta', 'Trento', 'Trieste', 'Venezia', 'Bologna', 'Firenze', 'Perugia', 'Ancona', 'Roma', 'Aquila', 'Campobasso', 'Napoli', 'Bari', 'Potenza', 'Catanzaro', 'Palermo', 'Cagliari']

for City in CITIES:
	df = pd.read_json(City + ".json")
	
	for i,w in enumerate(df['weather']):
		if w == "Smoke" or w == "Haze" or w == "Mist":
			df['weather'][i] = "Fog"
		if w == "Drizzle" or w == "Thunderstorm":
			df['weather'][i] = "Rain"
		
	df['hour'] = [dt.datetime.utcfromtimestamp(d).hour for d in df['ts']]

	df['weather_forecast'] = df['weather'].shift(3)

	df['temp_forecast'] = df['temp'].shift(3)

	df2 = df.drop(columns = ['name', 'ts'])

	df2 = df2.dropna()

	df2.to_csv("./Final/" + City + ".csv", index = False)

