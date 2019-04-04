import json, time

start = time.time()

# Translate avro message to NGSI
def to_ngsi(ms):
	#
	msg_time = time.ctime(ms["header"]["time"])
	id_string = "hmod.itcrowd.predator.uav.2:"+str(ms["header"]["time"])
	entity = {

		"id": id_string,
		"type":"UAV_simple",
		#"time" : str(msg_time),
	}

	value = dict()

	# The modules from which data is provided
	srcMod = {
		"Navigation" : "location",
		"Core" : "CPUUsage",
		"CPU"  : "CPUTime",
		# More attributes here
	}

	# Type for each attribute value
	types = {
		"location" : "geo:json",
		"CPUUsage" : "percent",
		"CPUTime"  : "sec",
		# More types here
	}

	attr = ms["header"]["sourceModule"]
	key = srcMod[attr]
	
	# translate the message
	# Special case for location attribute.
	if  attr == "Navigation":
		m = ms
		value[key] = {
			"value": {"type": "Point",
					"coordinates":[m['latitude'],m['longitude']]
					},
			"type": types[key]
		}
		value['elevation'] = {
			"value": m['height'],
			"type": "meter"
		}
		entity.update(value)
	else:
		value[key] = {
			"value":m["value"],
			"type":types[key]
		}
		entity.update(value)
	return entity

# messages.txt file contains the consumed avro messages.

ngsi_data=[]
with open('messages.txt') as f:
	for line in f:
		ngsi_data.append(to_ngsi(json.loads(line)))

print(len(ngsi_data),'messages translated...')


end = time.time()

print(json.dumps(ngsi_data[1], indent=4))
print("Execution time: ", (end-start),'seconds.')
