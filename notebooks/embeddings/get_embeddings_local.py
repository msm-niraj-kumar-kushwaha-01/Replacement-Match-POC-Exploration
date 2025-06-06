import requests

INFERENCE_URL = "http://localhost:8080/vectors"
payload = {"text": "foo bar"}

response = requests.post(INFERENCE_URL, json=payload)
print("Raw response:", response.text)
print("Embedding:", response.json().get("vector"))