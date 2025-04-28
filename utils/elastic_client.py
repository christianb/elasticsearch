import elasticsearch
import utils.env as env

def client():
	return elasticsearch.Elasticsearch(
		hosts=[env.host()],
		http_auth=(env.username(), env.password()),
		verify_certs=False,
	)
