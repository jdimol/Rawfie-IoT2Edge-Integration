from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
import  requests, time


# Avro Consumer
def avro_consumer(urls, topics, uav_name):
	
	c = AvroConsumer(urls)
	c.subscribe(topics)
	
	check_time = 0
	msges = []
	c_topic = ""
	loop = len(topics)
	
	while True:
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

	    if (m["header"]['sourceSystem'])== uav_name:

	    	if check_time==0 or check_time==m["header"]["time"]:
	    		c.unsubscribe()
	    		check_time=m["header"]["time"]
	    		c_topic = msg.topic()
	    		d = topics.index(c_topic)
	    		del topics[d]
	    		msges.append(msg)
	    		loop = loop - 1
	    		if loop==0:
	    			break
	    		c.subscribe(topics)
	    			    		
	c.close()
	
	# return the list of consumed avro messages (one for each topic - same timestamp)

	return(msges)