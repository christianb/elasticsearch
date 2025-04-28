import csv
from collections import deque
from elasticsearch import helpers
import utils.elastic_client as elastic
import datasets.movielens.movies as movies

def read_ratings():
	csvfile = open('../datasets/movielens/ml-latest-small/ratings.csv', 'r', encoding="utf-8")
	title_lookup = movies.read()
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
