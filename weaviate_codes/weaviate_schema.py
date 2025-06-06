import weaviate
from weaviate.classes.config import Configure, Property, DataType

client = weaviate.connect_to_local()

collection_name = "PartNumber"

try:
    # Delete existing collection if any
    if client.collections.exists(collection_name):
        client.collections.delete(collection_name)

    # Create collection with specified properties and OpenAI named vector
    client.collections.create(
        name=collection_name,
        properties=[
            Property(name="PartNumber", data_type=DataType.TEXT),
            Property(name="Brand", data_type=DataType.TEXT),
            Property(name="Category", data_type=DataType.TEXT),
            Property(name="type_input", data_type=DataType.TEXT),
            Property(name="env_value", data_type=DataType.TEXT),
            Property(name="specs", data_type=DataType.TEXT),
            Property(name="modified_specs", data_type=DataType.TEXT),  
            Property(name="mapper", data_type=DataType.TEXT)
            
        ],
        vectorizer_config=[
            Configure.NamedVectors.text2vec_openai(
                name="modified_specs_vector",
                source_properties=["modified_specs"]
            )
        ]
    )
    print("Collection created successfully!")
finally:
    client.close()