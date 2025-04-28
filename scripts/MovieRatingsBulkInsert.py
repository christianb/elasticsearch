import csv
from collections import deque
from elasticsearch import helpers
import utils.elastic_client as elastic

def read_movies():
	csvfile = open('../datasets/movielens/ml-latest-small/movies.csv', 'r', encoding="utf-8")
	reader = csv.DictReader(csvfile)
	title_lookup = {}
	for movie in reader:
		title_lookup[movie['movieId']] = movie['title']

	return title_lookup


def read_ratings():
	csvfile = open('../datasets/movielens/ml-latest-small/ratings.csv', 'r', encoding="utf-8")
	title_lookup = read_movies()
	reader = csv.DictReader(csvfile)

	for line in reader:
		rating = {}
		rating['user_id'] = int(line['userId'])
		rating['movie_id'] = int(line['movieId'])
		rating['title'] = title_lookup[line['movieId']]
		rating['rating'] = float(line['rating'])
		rating['timestamp'] = int(line['timestamp'])
		yield rating


elasticClient = elastic.client()

elasticClient.indices.delete(index="ratings", ignore=404)
deque(helpers.parallel_bulk(elasticClient, read_ratings(), index="ratings", request_timeout=300), 0)
elasticClient.indices.refresh()
