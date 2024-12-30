# fetch_data.py
import requests

def fetch_data(task_number):
    url = f"https://jsonplaceholder.typicode.com/posts/{task_number}"
    response = requests.get(url)
    print(f"Task {task_number} fetched data: {response.json()}")