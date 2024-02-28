import psycopg2
import redis
import pickle
import hashlib

conn = psycopg2.connect("dbname=RedisDb user=postgres password=root")
cur = conn.cursor()

r_server = redis.Redis("localhost")
sql = "SELECT * FROM bookTable WHERE pages > 50"
def validate_date():
    hash = hashlib.sha224()
