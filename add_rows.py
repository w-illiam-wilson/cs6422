import json

username = None
password = None

with open('config.json') as f:
    data = json.load(f)
    username = data['username']
    password = data['password']

print(username)
print(password)