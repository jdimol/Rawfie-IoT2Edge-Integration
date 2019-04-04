from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import json, time
import  requests

# The below fields have to be defined...

# Bootstrap server and schema registry urls (Avro Producer)
# urls = {
#     'bootstrap.servers': 'url:port',
#     'group.id': 'groupid',
#     'schema.registry.url': 'url:port'}

# Zookeaper topics
# topics = [] # ['topic_1', 'topic_2', 'topic_3', ..., 'topic_n']

# uav_name = '' # The name of the UAV 

# Avro Consumer
# Consume many avro messages together and save them into a list

def avro_consumer(urls, topics, uav_name, duration):

	c = AvroConsumer(urls)
	c.subscribe(topics)

	msges = [] # list of avro messages
	start_time = time.time()

	while (time.time()-start_time) <= duration:
	    try:
	        msg = c.poll(10)
	    except SerializerError as e:
	      #  print("Message deserialization failed for {}: {}".format(msg, e))
	        break

	    if msg is None:
	        continue

	    if msg.error():
	      #  print("AvroConsumer error: {}".format(msg.error()))
	        continue
	    m = msg.value()
	    print(m)
	    time.sleep(1)
	    if (m["header"]['sourceSystem'])==uav_name:
	    	
	    	msges.append(msg)
	    	
	c.close()

	return msges


# Main call
duration = 10	# seconds
measurements = avro_consumer(urls, topics, uav_name, duration)
print(measurements)