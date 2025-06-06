from weaviate import connect_to_local
from weaviate.classes.query import Filter, MetadataQuery
import time
import json

def find_matches_by_specs(specs_query: str, type_output: str):
    client = connect_to_local()
    collection = client.collections.get("PartNumber")

    results = collection.query.near_text(
        query=specs_query,
        limit=10,
        filters=Filter.by_property("type_input").equal(type_output),
        return_metadata=MetadataQuery(distance=True)
    )
    client.close()

    matches = []
    for r in results.objects:
        matches.append({
            "partNumber": r.properties["partNumber"],
            "distance": r.metadata.distance
        })
    return matches
