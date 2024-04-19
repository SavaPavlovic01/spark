from flask import Flask, Response
import time
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


url = 'https://randomuser.me/api/'

url_spotify = 'https://api.spotify.com/v1/browse/new-releases'

offset = 0

app = Flask(__name__)

def get_token():
    token_url = 'https://accounts.spotify.com/api/token'
    resp = requests.post(token_url, {'grant_type':"client_credentials", 'client_id':CLIENT_ID, 'client_secret':CLIENT_SECRET})
    return resp.json()["access_token"]

def make_user():
    data = requests.get(url).json()
    return json.dumps(data)

def get_album_data(token,id):
    header = {
        'Authorization': 'Bearer {token}'.format(token=token)
    }
    data_url = 'https://api.spotify.com/v1/albums/{id}'.format(id = id)
    resp = requests.get(data_url, headers=header)
    #print(json.dumps(resp.json(), indent=4))
    return json.dumps(resp.json())

def get_album(token):
    header = {
        'Authorization': 'Bearer {token}'.format(token=token)
    }

    global offset
    data = requests.get(url_spotify, params={'limit':1, "offset":offset}, headers=header).json()
    album_id = data["albums"]["items"][0]['id']
    offset += 1
    return get_album_data(token, album_id)

@app.route("/users.json")
def get_users():
    def generate():
        token = get_token()
        while True:
            yield get_album(token)
            time.sleep(1)
    return Response(generate())

if __name__ == "__main__":
    app.run(debug=True)