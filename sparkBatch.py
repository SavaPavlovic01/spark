from pyspark import *
import json

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

if __name__ == "__main__":
    conf = SparkConf().setAppName("Batch").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    lines = sc.textFile("./temp/data.txt").map(lambda line: json.loads(line))
    



