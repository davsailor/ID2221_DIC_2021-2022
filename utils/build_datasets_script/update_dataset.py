from urllib.request import urlopen
import time

cities = ['Milano', 'Torino', 'Genova', 'Aosta', 'Trento', 'Trieste', 'Venezia', 'Bologna', 'Firenze', 'Perugia', 'Ancona', 'Roma', 'Aquila', 'Campobasso', 'Napoli', 'Bari', 'Potenza', 'Catanzaro', 'Palermo', 'Cagliari']
coordinates = [[45.4642, 9.1895], [45.0704, 7.6868], [44.4264, 8.9151], [45.7376, 7.3172], [46.0678, 11.1210], [45.6432, 13.7903], [45.4371, 12.3326], [44.4938, 11.3387], [43.7792, 11.2462], [43.1122, 12.3887], [43.5942, 13.5033], [41.8919, 12.5113], [42.3505, 13.3995], [41.5594, 14.6673], [40.8563, 14.2464], [41.1114, 16.8554], [40.6417, 15.8079], [38.8824, 16.6008], [38.1320, 13.3356], [39.2305, 9.1191]]
key = 'c1ebde1f788ab3675ef301f7e4ea8855'
hourly = '"hourly"'
timestamp = int(time.time() - 24*60*60)
#timestamp = 1634022000

for count in range(len(cities)):
	filename = f"datasets/{cities[count]}.json"
	with open(filename, 'r') as f:
		data = f.read()
		f.close()
	url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={coordinates[count][0]}&lon={coordinates[count][1]}&dt={timestamp}&appid={key}"
	raw = urlopen(url).read().decode()
	data = data[:-2]
	data += ','
	tmp1 = raw.split('"hourly":[')
	tmp1 = tmp1[1]
	data += tmp1
	with open(filename, 'w') as f:
		data = f.write(data)
		f.close()
	time.sleep(1)
