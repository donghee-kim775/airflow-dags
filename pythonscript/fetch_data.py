# fetch_data.py
import requests
import sys

def fetch_data(task_number):
    url = f"https://jsonplaceholder.typicode.com/posts/{task_number}"
    response = requests.get(url)
    print(f"Task {task_number} fetched data: {response.json()}")
    
if __name__ == "__main__":
    if len(sys.argv) > 1:
        task_number = sys.argv[1]
        fetch_data(task_number)
    else:
        print("No task number provided.")