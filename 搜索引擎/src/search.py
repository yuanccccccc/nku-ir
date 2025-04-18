# 搜索模块基本在这里实现

from elasticsearch import Elasticsearch
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# Initialize Elasticsearch client
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

index_name = "web_pages"


def tokenize(text):

    # 分词处理
    text = text[:30000]

    response = es.indices.analyze(
        index=index_name, body={"text": text, "analyzer": "ik_smart"}
    )
    return [token["token"] for token in response["tokens"]]


def is_url(query):
    """判断输入是否为 URL（简单地通过检查是否以 http:// 或 https:// 开头）。"""
    return query.startswith("http://") or query.startswith("https://")


def search_url(query):
    """使用 'term' 查询进行 URL 精确匹配搜索。"""
    print("查询 URL 结果如下：")
    response = es.search(
        index=index_name,
        body={
            "query": {"term": {"url": query}},
            "size": 1000,
        },
    )
    return response


def search_phrase(query, college):
    """使用 'match_phrase' 查询，并将 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [  # 必须匹配 string1
                        {
                            "multi_match": {
                                "query": query,  # 字符串1
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 3.0,  # 字符串1的权重
                            }
                        }
                    ],
                    "should": [  # 可选匹配 string2，提高分数
                        {
                            "multi_match": {
                                "query": college,  # 字符串2
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 2,  # 字符串2的权重
                            }
                        }
                    ],
                    "minimum_should_match": 0,  # should 子句可以不匹配
                }
            },
            "size": 1000,
        },
    )

    return response


def search_match(query_text, college):
    """使用 'match' 查询进行多词匹配，并将 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query_text,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 3,
                            }
                        }
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": college,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 2,
                            }
                        }
                    ],
                    "minimum_should_match": 0,
                }
            },
            "size": 1000,
        },
    )

    return response


def search_wildcard(query_text, college):
    """使用 'wildcard' 查询进行通配符匹配，并将 college 添加为加权因子"""
    response = es.search(
        index=index_name,
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "wildcard": {
                                "title": {
                                    "value": query_text,
                                    "boost": 3.0,  # 标题权重最大为4
                                }
                            }
                        }
                    ],
                    "should": [
                        {
                            "multi_match": {
                                "query": college,
                                "fields": [
                                    "title^7",
                                    "content^3",
                                    "anchors.anchor_text^2",
                                ],
                                "boost": 2,
                            }
                        }
                    ],
                    "minimum_should_match": 0,
                },
            },
            "size": 1000,
        },
    )
    return response


def get_pagerank(urls):
    """检索每个 URL 的 PageRank 值。"""
    pagerank_values = {}
    for url in urls:
        response = es.search(index="pagerank", body={"query": {"match": {"url": url}}})
        if response["hits"]["hits"]:
            pagerank_values[url] = response["hits"]["hits"][0]["_source"]["pagerank"]
        else:
            pagerank_values[url] = 0.0
    return pagerank_values


def process_results(response):
    """处理 Elasticsearch 搜索结果。"""
    documents = []
    titles = []
    anchor_texts = []
    urls = []
    for hit in response["hits"]["hits"]:
        documents.append(hit["_source"]["content"])
        titles.append(hit["_source"]["title"])
        anchors = hit["_source"].get("anchors", [])
        anchor_texts.append(" ".join([anchor["anchor_text"] for anchor in anchors]))
        urls.append(hit["_source"]["url"])
    return documents, titles, anchor_texts, urls


def merge_results(results_list):
    """合并多个查询结果并按得分排序，同时进行去重"""
    unique_results = {}  # 用于存储唯一的文档，键是文档的 URL
    for result in results_list:
        for hit in result["hits"]["hits"]:
            # 获取文档的 URL
            doc_url = hit["_source"]["url"]

            # 如果文档的 URL 不在 unique_results 中，或者当前得分更高，则更新
            if (
                doc_url not in unique_results
                or hit["_score"] > unique_results[doc_url]["_score"]
            ):
                unique_results[doc_url] = hit

    # 将去重后的文档按得分排序
    sorted_results = sorted(
        unique_results.values(), key=lambda x: x["_score"], reverse=True
    )
    return sorted_results


def search_file(query):
    response = es.search(
        index="fileindex", body={"query": {"match": {"content": query}}, "size": 100}
    )
    results = []
    for hit in response["hits"]["hits"]:
        file_url = hit["_source"]["url"]
        results.append((file_url, "下载链接"))
    return results if results else None


def all_search(query, college, qtype):
    if qtype == "file":
        return search_file(query)
    return search_and_rank(query, college)


def search_and_rank(query, college=None):
    """处理查询并对结果进行排序的主搜索函数。"""
    print(f"Original query: {query}")
    if is_url(query):
        url_response = search_url(query)
        if url_response["hits"]["hits"]:
            title = url_response["hits"]["hits"][0]["_source"]["title"]
            return [(query, title)]
        else:
            return None
    # 第一步：将查询分割成多个部分
    query_parts = query.split(" ")

    # 第二步：处理每个查询部分
    results_list = []
    for part in query_parts:
        print(f"处理查询部分: {part}")
        if "*" in part or "?" in part:
            print("进入到了通配符查询部分")
            wildcard_response = search_wildcard(part, college)
            results_list.append(wildcard_response)
        else:
            # 标准分词查询
            phrase_response = search_phrase(part, college)
            if phrase_response["hits"]["hits"]:
                print("进入到了短语查询部分")
                results_list.append(phrase_response)
                if len(phrase_response["hits"]["hits"]) > 100:
                    break

            print("进入到了多词查询部分")
            match_response = search_match(part, college)
            results_list.append(match_response)

    # 第三步：合并查询结果
    merged_results = merge_results(results_list)

    urls = [hit["_source"]["url"] for hit in merged_results]
    titles = [hit["_source"]["title"] for hit in merged_results]
    scores = [hit["_score"] for hit in merged_results]
    pagerank_values = get_pagerank(urls)

    combined_scores = []
    for i, url in enumerate(urls):
        combined_score = (
            scores[i] * 0.001 + pagerank_values[url] * 1000
        )  # Pagerank 权重为1.0
        combined_scores.append((url, titles[i], combined_score))

    # 根据综合得分排序
    sorted_scores = sorted(combined_scores, key=lambda x: x[2], reverse=True)
    return [(url, title) for url, title, score in sorted_scores]


# # Example usage
# query = "南开大学"
# results = search_and_rank(query)
# for url, score in results:
#     print(f"URL: {url}, Score: {score}")
