import csv
from collections import deque
from elasticsearch import helpers
import utils.elastic_client as elastic
import datasets.movielens.movies as movies

def read_tags():
	csvfile = open('../datasets/movielens/ml-latest-small/tags.csv', 'r', encoding="utf-8")
	title_lookup = movies.read()
	reader = csv.DictReader(csvfile)

	for line in reader:
		tag = {
			'user_id': int(line['userId']),
			'movie_id': int(line['movieId']),
			'title': title_lookup[line['movieId']],
			'tag': line['tag'],
			'timestamp': int(line['timestamp'])}
		yield tag


elasticClient = elastic.client()

index="tags"

elasticClient.indices.delete(index=index, ignore=404)
deque(helpers.parallel_bulk(elasticClient, read_tags(), index="tags", request_timeout=300), 0)
elasticClient.indices.refresh()
