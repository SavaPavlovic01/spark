from pyspark import *
import json
import datetime
import requests

def count_markets(lines):
    cnt_markets = lines.flatMap(lambda line: [(market, 1) for market in line['markets']]).reduceByKey(lambda l, r: l+r )
    return cnt_markets.collect()

def count_genres(lines):
    cnt_genres = lines.flatMap(lambda line: [(market, 1) for market in line['genres']]).reduceByKey(lambda l, r: l+r )
    return cnt_genres.collect()

def avg_popularity(lines):
    return lines.reduce(lambda l,r : {'popularity':l['popularity'] + r['popularity'], 'cnt':l.get('cnt', 1) + r.get('cnt',1)})

def count_by_date(lines):
    temp = lines.map(lambda line: (line['release_date'], 1)).reduceByKey(lambda l,r: l+r)
    return temp.collect()

def redcue(l, r):
    ret = {}
    ret['cnt'] = l['cnt'] + r['cnt']
    ret['avg_popularity'] = l['avg_popularity'] + r['avg_popularity']
    ret['min_popularity'] = l['min_popularity'] if l['min_popularity'] < r['min_popularity'] else r['min_popularity']
    ret['max_popularity'] = l['max_popularity'] if l['max_popularity'] > r['max_popularity'] else r['max_popularity']

    ret['genres_cnt'] = {}
    for key in l['genres_cnt'].keys():
        ret['genres_cnt'].setdefault(key, l['genres_cnt'][key] + r['genres_cnt'].get(key, 0))
    for key in r['genres_cnt'].keys():
        ret['genres_cnt'].setdefault(key, r['genres_cnt'][key])

    ret['album_types'] = {}
    for key in l['album_types'].keys():
        ret['album_types'].setdefault(key, l['album_types'][key] + r['album_types'].get(key, 0))
    for key in r['album_types'].keys():
        ret['album_types'].setdefault(key, r['album_types'][key])

    ret['release_date'] = l['release_date']
    return ret


def data_by_date(lines): 
    temp = lines.map(lambda line: (line['release_date'], { 'cnt':1,
                                                           'genres_cnt':{genre:1 for genre in line['genres']},
                                                           'album_types':{line['album_type']:1},
                                                            'avg_popularity': line['popularity'],
                                                            'max_popularity':line['popularity'],
                                                            'min_popularity':line['popularity'],
                                                            'release_date':line['release_date']
                                                           }))
    final = temp.reduceByKey(lambda l,r: redcue(l,r))
    return final.collect()

def data_by_month(lines):
    temp = lines.map(lambda line: (line['release_date'].split("-")[1], { 'cnt':1,
                                                           'genres_cnt':{genre:1 for genre in line['genres']},
                                                           'album_types':{line['album_type']:1},
                                                            'avg_popularity': line['popularity'],
                                                            'max_popularity':line['popularity'],
                                                            'min_popularity':line['popularity'],
                                                            'release_date':line['release_date']
                                                           }))
    final = temp.reduceByKey(lambda l,r: redcue(l,r))
    return final.collect()

def data_by_day(lines):
    temp = lines.map(lambda line: (line['release_date'].split("-")[2], { 'cnt':1,
                                                           'genres_cnt':{genre:1 for genre in line['genres']},
                                                           'album_types':{line['album_type']:1},
                                                            'avg_popularity': line['popularity'],
                                                            'max_popularity':line['popularity'],
                                                            'min_popularity':line['popularity'],
                                                            'release_date':line['release_date']
                                                           }))
    final = temp.reduceByKey(lambda l,r: redcue(l,r))
    return final.collect()

def send_to_db(data):
    url = 'http://127.0.0.1:5000/insertDay'
    data_to_send = {
        'date': data['release_date'],
        'cnt':data['cnt'],
        'min_pop':data['min_popularity'],
        'max_pop':data['max_popularity'],
        'avg_pop':data['avg_popularity'],
    }
    genres = ""
    album_types = ""
    first = True
    for key in data['genres_cnt'].keys():
        if not first: 
            genres += ',' 
        genres += key + ":" + str(data['genres_cnt'][key])
        first = False
    
    first = True
    for key in data['album_types'].keys():
        if not first: 
            album_types += ','
        album_types += key + ":" + str(data['album_types'][key])
        first = False

    data_to_send['genres'] = genres
    data_to_send['album_types'] = album_types

    requests.post(url, json=data_to_send)

def send_all_to_db(data):
    for tuple in data:
        send_to_db(tuple[1])

def dump_to_sv(data, seperator = ","):
    
    all_genres = set()
    all_album_types = set()

    genres_list = []
    all_types_list = []
    for tuple in data:
        for genre in tuple[1]['genres_cnt']:
            all_genres.add(genre)
            
        for album_type in tuple[1]['album_types']:
            all_album_types.add(album_type)
    
    genres_list = list(all_genres)
    all_types_list = list(all_album_types)
    with open('processed/data{timestamp}.csv'.format(timestamp = datetime.datetime.now()), 'w') as output_file:
        header = "date,cnt,min_pop,max_pop,avg_pop"
        for k in genres_list:
            header += ("," + k)
        for k in all_types_list:
            header += ("," + k)
        output_file.write(header + "\n")

        for tuple in data:
            output_file.write(tuple[0] + ",")
            output_file.write(str(tuple[1]['cnt']) + ",")
            output_file.write(str(tuple[1]['min_popularity']) + ",")
            output_file.write(str(tuple[1]['max_popularity']) + ",")
            output_file.write(str(tuple[1]['avg_popularity']) + ",")
            for genre in genres_list:
                output_file.write(str(tuple[1]['genres_cnt'].get(genre, 0)) + ",")
            for type in all_types_list:
                output_file.write(str(tuple[1]['album_types'].get(type, 0)) + ",")
            output_file.write("\n")
       


if __name__ == "__main__":
    conf = SparkConf().setAppName("Batch").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("./temp/data.txt").map(lambda line: json.loads(line))
    data = data_by_day(lines)
    print(data)
    send_all_to_db(data)


