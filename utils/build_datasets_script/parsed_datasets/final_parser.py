cities = ["Milano", "Torino", "Genova", "Aosta", "Trento", "Trieste", "Venezia", "Bologna", "Firenze", "Perugia", "Ancona", "Roma", "Aquila", "Campobasso", "Napoli", "Bari", "Potenza", "Catanzaro", "Palermo", "Cagliari"]


for city in cities:
	input = "./0_init/"+city+".json"
	fi = open(input, "r")
	data = fi.readlines()
	fi.close()

	data = map(lambda x: x.rstrip(), data)
	joined_data = ",".join(data)
	#joined_data = joined_data[:-1]
	jsonstr = "[" + joined_data + "]"

	output = "./1_final/"+city+".json"
	fo = open(output, "w")
	fo.write(jsonstr)
	fo.close()
