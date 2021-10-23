import pandas as pd

CITIES = ['Torino', 'Genova', 'Aosta', 'Trento', 'Trieste', 'Venezia', 'Bologna', 'Firenze', 'Perugia', 'Ancona', 'Roma', 'Aquila', 'Campobasso', 'Napoli', 'Bari', 'Potenza', 'Catanzaro', 'Palermo', 'Cagliari']

df = pd.read_csv("Milano.csv")

for City in CITIES:
	df = df.append(pd.read_csv(City + ".csv"))
	
df.to_csv("Dataset.csv", index = False)
