# 这个文件用于根据索引web_pages来构建一个有向图并计算PageRank，将PageRank和url一起存入索引pagerank中

import networkx as nx
from elasticsearch import Elasticsearch, helpers

# 初始化 Elasticsearch 客户端
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

index_name = "web_pages"
pagerank_index_name = "pagerank"
# Create the new index for PageRank values
index_settings = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {"url": {"type": "keyword"}, "pagerank": {"type": "float"}}
    },
}
if es.indices.exists(index=pagerank_index_name):
    es.indices.delete(index=pagerank_index_name)
es.indices.create(index=pagerank_index_name, body=index_settings)

# Query to get all documents
query = {"query": {"match_all": {}}}

# 从 Elasticsearch 获取所有文档
response = es.search(
    index=index_name, body=query, size=10000, scroll="5m", request_timeout=120
)
# print("Initial search response:", response)
print(f"Number of records in response: {len(response['hits']['hits'])}")
# 创建有向图
G = nx.DiGraph()

# Scroll through all documents
scroll_id = response["_scroll_id"]

while True:
    print("Processing scroll batch...")
    for hit in response["hits"]["hits"]:
        source_url = hit["_source"]["url"]
        # Extract anchor texts and target URLs from the document
        anchors = hit["_source"].get("anchors", [])
        for anchor in anchors:
            target_url = anchor.get("target_url")
            if target_url:
                G.add_edge(source_url, target_url)
                # print(f"Added edge from {source_url} to {target_url}")

    if "hits" not in response or not response["hits"]["hits"]:
        break

    scroll_id = response["_scroll_id"]
    try:
        response = es.scroll(scroll_id=scroll_id, scroll="5m", request_timeout=120)
    except Elasticsearch.NotFoundError:
        print("Scroll ID not found or expired. Exiting scroll loop.")
        break

# 计算 PageRank
pagerank = nx.pagerank(G)

# Prepare bulk index actions for the new index
actions = []
for url, rank in pagerank.items():
    action = {"_index": pagerank_index_name, "_source": {"url": url, "pagerank": rank}}
    actions.append(action)
    print(f"Prepared action for URL: {url}, PageRank: {rank}")
print("action", len(actions))
# 批量索引 PageRank 值
success, failed = helpers.bulk(es, actions, chunk_size=100, stats_only=True)
print(f"Bulk indexing completed. Success: {success}, Failed: {failed}")
# 查询以获取 pagerank_test 索引中的所有文档
query = {"query": {"match_all": {}}}

# for hit in response["hits"]["hits"]:
#     for hit in response["hits"]["hits"]:
#         print(f"URL: {hit['_source']['url']}")
#         print(f"PageRank: {hit['_source']['pagerank']}")
# 查询以获取 pagerank_test 索引中的文档总数


count_response = es.count(index=pagerank_index_name)
# 查询以获取 web_pages_test 索引中的文档总数
web_pages_count_response = es.count(index=index_name)
print(f"Total number of documents in {index_name}: {web_pages_count_response['count']}")
print(f"Total number of documents in {pagerank_index_name}: {count_response['count']}")
