import csv

def read():
	csvfile = open('../datasets/movielens/ml-latest-small/movies.csv', 'r', encoding="utf-8")
	reader = csv.DictReader(csvfile)
	title_lookup = {}
	for movie in reader:
		title_lookup[movie['movieId']] = movie['title']

	return title_lookup