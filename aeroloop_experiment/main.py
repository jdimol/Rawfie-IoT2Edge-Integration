import requests, avro_consumer, ngsi_translator



# The below fields have to be defined...

# Bootstrap server and schema registry urls (Avro Producer)
# urls = {
#     'bootstrap.servers': 'url:port',
#     'group.id': 'groupid',
#     'schema.registry.url': 'url:port'}

# Zookeaper topics
# topics = [] # ['topic_1', 'topic_2', 'topic_3', ..., 'topic_n']

# uav_name = '' # The name of the UAV 



# POST NGSI data
def posttoOrion(data):
      
      headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

      url = 'url' # Url has to be defined.
      
      r = requests.post(url, json=data, headers=headers)
      
      print(r.status_code)
  
      return r


# Main Section of the experiment
ms = avro_consumer(urls, topics, uav_name)
data = to_ngsi(ms)
response = posttoOrion(data)