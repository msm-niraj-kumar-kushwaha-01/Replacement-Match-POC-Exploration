from weaviate import connect_to_local
client = connect_to_local()
collection = client.collections.get("PartNumber")
response = collection.query.fetch_objects(include_vector=True)
print(len(response.objects))
for o in response.objects:
    print(o.properties)  # Prints the properties of each object
    print(o.vector)      # Prints the vector of each object

client.close()