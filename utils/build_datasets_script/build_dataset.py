from urllib.request import urlopen
import time
from time import sleep

CITIES = ['Milano', 'Torino', 'Genova', 'Aosta', 'Trento', 'Trieste', 'Venezia', 'Bologna', 'Firenze', 'Perugia', 'Ancona', 'Roma', 'Aquila', 'Campobasso', 'Napoli', 'Bari', 'Potenza', 'Catanzaro', 'Palermo', 'Cagliari']
COORDINATES = [[45.4642, 9.1895], [45.0704, 7.6868], [44.4264, 8.9151], [45.7376, 7.3172], [46.0678, 11.1210], [45.6432, 13.7903], [45.4371, 12.3326], [44.4938, 11.3387], [43.7792, 11.2462], [43.1122, 12.3887], [43.5942, 13.5033], [41.8919, 12.5113], [42.3505, 13.3995], [41.5594, 14.6673], [40.8563, 14.2464], [41.1114, 16.8554], [40.6417, 15.8079], [38.8824, 16.6008], [38.1320, 13.3356], [39.2305, 9.1191]]
KEY = 'c1ebde1f788ab3675ef301f7e4ea8855'

hourly = '"hourly"'

base = int(time.time() - 5*60*60*24)
timestamps = [i*base for i in range(1, 6)]

for count in range(len(CITIES)):
	first = True
	data = ''
	for timestamp in timestamps:
		url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={COORDINATES[count][0]}&lon={COORDINATES[count][1]}&dt={timestamp}&appid={KEY}"
		raw = urlopen(url).read().decode()
		if first:
			first = False
			tmp1 = raw.split('"current"')
			tmp2 = tmp1[1].split('"hourly"')
			data = tmp1[0] + hourly + tmp2[1]
			data = data[:-2]
		else:
			tmp1 = raw.split('"hourly":[')
			tmp1 = tmp1[1]
			tmp1 = tmp1[:-2]
			data += tmp1
		data += ','
	data = data[:-1]
	data += ']}'
	filename = f"datasets/{CITIES[count]}.json"
	with open(filename, 'x') as f:
		f.write(data)
		f.close()
	sleep(1)
