from weaviate.util import generate_uuid5

properties = {
    "brand": "BrandX",
    "category": "CatX",
    "product_number": "PNTEST06"
}
uuid = generate_uuid5(properties)
print(uuid)
# Use this uuid when inserting the object