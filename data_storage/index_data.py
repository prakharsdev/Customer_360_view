from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

def index_data(es, index_name, data):
    actions = [
        {
            "_index": index_name,
            "_source": doc
        }
        for doc in data
    ]
    bulk(es, actions)

if __name__ == "__main__":
    es = Elasticsearch(['localhost:9200'])

    # Example usage:
    with open("path/to/customer_data.json") as f:
        customer_data = json.load(f)
    index_data(es, "customer_index", customer_data)
