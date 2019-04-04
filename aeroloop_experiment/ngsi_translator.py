import json, time

# Translate avro messages to NGSI
def to_ngsi(ms):
	#
	msg_time = time.ctime(ms[0].value()["header"]["time"])
	id_string = "aeroloop.wizzit.venac.9:"+str(ms[0].value()["header"]["time"])
	entity = {

		"id": id_string,
		"type":"UAV_simple",
	}

	value = dict()

	for msg in ms:
		
		if msg.topic() == "aeroloop_Location":
			m = msg.value()
			value['location'] = {
				"value": {"type": "Point",
						"coordinates":[m['latitude'],m['longitude']]
						},
				"type": "geo:json"
			}
			value['elevation'] = {
				"value": m['height'],
				"type": "meter"
			}
			entity.update(value)
		else:
		#if msg.topic() == "aeroloop_CpuUsage":
			m = msg.value()
			value['CPUUsage'] = {
				"value": m['value'],
				"type": "percent"
			}
			entity.update(value)
	return entity