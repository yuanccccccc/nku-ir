import networkx as nx
import csv
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup

# 初始化 Elasticsearch 客户端
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])
index_name = "web_pages_test"
pagerank_index_name = "pagerank_test"
# 为 PageRank 值创建新索引
index_settings = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {"url": {"type": "keyword"}, "pagerank": {"type": "float"}}
    },
}
if es.indices.exists(index=pagerank_index_name):
    es.indices.delete(index=pagerank_index_name)
es.indices.create(index=pagerank_index_name, body=index_settings)

# 查询以获取所有文档
query = {"query": {"match_all": {}}}

# 从 Elasticsearch 获取所有文档
response = es.search(
    index=index_name, body=query, size=10000, scroll="5m", request_timeout=120
)
# print("Initial search response:", response)
print(f"Number of records in response: {len(response['hits']['hits'])}")
# 创建有向图
G = nx.DiGraph()

# 滚动获取所有文档
scroll_id = response["_scroll_id"]

while True:
    print("Processing scroll batch...")
    for hit in response["hits"]["hits"]:
        source_url = hit["_source"]["url"]
        # 从文档中提取锚文本和目标 URL
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
print("Computed PageRank:", pagerank)

# 为新索引准备批量索引操作
actions = []
usdurl = set()
for url, rank in pagerank.items():
    assert url not in usdurl
    assert url not in usdurl
    usdurl.add(url)
    action = {"_index": pagerank_index_name, "_source": {"url": url, "pagerank": rank}}
    actions.append(action)
    print(f"Prepared action for URL: {url}, PageRank: {rank}")
print("action", len(actions))
print("urllen", len(usdurl))
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
