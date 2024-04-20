from kafka import KafkaConsumer
import json
import random

batch_size = 100

genres = ["rap", "pop", "noise", "rock", "punk", "post-punk", "post-rock", "prog-rock", "edm", "dubstep", "house", "experimental"]

def transform_album(album):
    album_trimed = {}
    album_trimed['album_type'] = album['album_type']

    artists = []
    for artist in album['artists']:
        artists.append((artist['id'], artist['name']))
    album_trimed['artists'] = artists
    
    album_trimed['markets'] = album['available_markets']

    album_trimed['genres'] = album['genres']
    if len(album_trimed['genres']) == 0:
        album_trimed['genres'].append(random.choice(genres))
        
    album_trimed['album_name'] = album['name']
    
    album_trimed['release_date'] = album['release_date']

    album_trimed['popularity'] = album['popularity']

    tracks = []
    for track in album['tracks']['items']:
        tracks.append((track['name'], track['duration_ms']))
    album_trimed['tracks'] = tracks
    return album_trimed

def write_file(albums):
    file = open("./temp/data.txt", "w")
    for album in albums:
        file.write(json.dumps(album))
        file.write('\n');
    file.close()

if __name__ == "__main__":
    consumer = KafkaConsumer('users', group_id = 'batch', bootstrap_servers = ['localhost:9092'],
                              value_deserializer = lambda x: x.decode())
    
    new_albums = []
    while len(new_albums) < batch_size:
        data = consumer.poll(20, batch_size)
        if data:
            for _, message in data.items():
                for msg in message:
                    new_albums.append(transform_album(json.loads(msg.value)))
                    print(str(len(new_albums)) + "/" + str(batch_size))
            consumer.commit()
    write_file(new_albums)
