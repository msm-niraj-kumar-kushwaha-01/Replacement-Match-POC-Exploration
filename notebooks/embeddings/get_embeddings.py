import openai
import time



texts = [
    "Qdrant is the best vector search engine!",
    "Loved by Enterprises and everyone building for low latency, high performance, and scale.",
    "Vector search enables powerful semantic search capabilities.",
    "OpenAI provides state-of-the-art embedding models.",
    "Scaling search systems requires efficient indexing.",
    "Qdrant supports real-time similarity search at scale.",
    "Embedding vectors capture the meaning of text data.",
    "Low latency responses are critical for user experience.",
    "High dimensional vectors represent complex data features.",
    "Combining OpenAI embeddings with Qdrant improves search accuracy.",
    "Binary quantization reduces embedding storage requirements.",
    "Machine learning models require large datasets for training.",
    "Cloud infrastructure offers scalable computing resources.",
    "Natural language processing advances improve search relevance.",
    "Distributed databases enable high availability and throughput.",
    "Embedding models transform text into numerical vectors.",
    "Semantic similarity helps find related documents quickly.",
    "Efficient nearest neighbor search algorithms power recommendations.",
    "Qdrant integrates easily with existing data pipelines.",
    "Data indexing strategies affect retrieval speed.",
    "Large-scale vector search powers AI applications.",
    "Modern APIs simplify embedding generation and management.",
    "Open source projects foster collaboration and innovation.",
    "Embedding dimensionality impacts performance and accuracy.",
    "Real-time data updates require dynamic index handling.",
    "Combining multiple modalities enriches search results.",
    "Embedding models continue to improve with research progress.",
    "Data privacy is important when working with embeddings.",
    "Cloud-native vector databases offer flexibility and scalability.",
    "Vector search is transforming how we explore data.",
]

start_time = time.time()

response = openai.embeddings.create(
    input=texts,
    model="text-embedding-3-small"
)

end_time = time.time()

embeddings = [item.embedding for item in response.data]

print(f"Time taken to fetch embeddings: {end_time - start_time:.2f} seconds")

for i, emb in enumerate(embeddings):
    print(f"Embedding for text {i} (length {len(emb)}): {emb[:5]}...")
