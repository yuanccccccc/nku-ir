import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

index_name = "photo_in"


def searchphoto(url):
    response = es.search(
        index=index_name,
        body={
            "query": {"term": {"url": url}},
            "size": 100,
        },
    )
    hits = response["hits"]["hits"]
    if hits:
        return hits[0]["_source"]["filepath"].replace("\\", "/")
    return None


# response = es.search(
#     index=index_name,
#     body={
#         "query": {"match_all": {}},
#         "size": 10,  # Adjust the size as needed
#     },
# )
# hits = response["hits"]["hits"]
# for hit in hits:
#     print(f"URL: {hit['_source']['url']}, Filepath: {hit['_source']['filepath']}")
