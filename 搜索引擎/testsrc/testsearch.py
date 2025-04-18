from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from elasticsearch import Elasticsearch

# 初始化 Elasticsearch 客户端
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])


def tokenize(text):
    """使用 IK 分词器对文本进行分词。"""
    response = es.indices.analyze(
        index="web_pages_test", body={"text": text, "analyzer": "ik_smart"}
    )
    return [token["token"] for token in response["tokens"]]


def is_url(query):
    """判断输入是否为 URL（简单地通过检查是否以 http:// 或 https:// 开头）。"""
    return query.startswith("http://") or query.startswith("https://")


def search_url(query):
    """使用 'term' 查询进行 URL 精确匹配搜索。"""
    print("查询 URL 结果如下：")
    response = es.search(
        index="web_pages_test",
        body={
            "query": {"term": {"url": query}},
            "size": 1000,
        },
    )
    return response


def search_phrase(query):
    """使用 'match_phrase' 查询进行精确短语匹配搜索。"""
    response = es.search(
        index="web_pages_test",
        body={
            "query": {
                "match_phrase": {
                    "content": {"query": query, "boost": 2.0}  # 提升精确匹配的权重
                }
            },
            "size": 1000,
        },
    )
    return response


def search_match(query_text):
    """使用 'match' 查询进行多词匹配搜索。"""
    response = es.search(
        index="web_pages_test",
        body={
            "query": {
                "multi_match": {
                    "query": query_text,
                    "fields": ["title^7", "content^2", "anchors.anchor_text^2"],
                }
            },
            "size": 1000,
        },
    )
    return response


def search_wildcard(query_text):
    """使用 'wildcard' 查询进行通配符匹配搜索。"""
    response = es.search(
        index="web_pages_test",
        body={
            "query": {
                "wildcard": {
                    "content.raw": {
                        "value": f"*{query_text}*",
                        "boost": 1.0,  # 提升通配符匹配的权重
                    }
                }
            },
            "size": 1000,
        },
    )
    return response


def get_pagerank(urls):
    """检索每个 URL 的 PageRank 值。"""
    pagerank_values = {}
    for url in urls:
        response = es.search(
            index="pagerank_test", body={"query": {"match": {"url": url}}}
        )
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


def search_and_rank(query):
    """处理查询并对结果进行排序的主搜索函数。"""
    print(f"Original query: {query}")

    # 第一步：将查询分割成多个部分
    query_parts = query.split(" ")

    # 第二步：处理每个查询部分
    results_list = []
    for part in query_parts:
        # 根据每个部分进行递归处理
        print(f"处理查询部分: {part}")
        if is_url(part):
            # 如果是URL，执行精确URL查询
            url_response = search_url(part)
            results_list.append(url_response)
        elif "*" in part or "?" in part:
            # 如果包含通配符，执行通配符查询
            print("进行通配符查询")
            wildcard_response = search_wildcard(part)
            results_list.append(wildcard_response)
        else:
            # 否则执行标准的分词查询
            tokens = tokenize(part)
            query_text = " ".join(tokens)
            print(f"Tokenized query: {query_text}")
            phrase_response = search_phrase(part)
            if phrase_response["hits"]["hits"]:
                results_list.append(phrase_response)
            else:
                match_response = search_match(query_text)
                if match_response["hits"]["hits"]:
                    results_list.append(match_response)
                else:
                    # 如果都没有匹配的结果，则跳过
                    continue

    # 第三步：合并查询结果
    merged_results = merge_results(results_list)

    # 第四步：处理文档内容并计算 TF-IDF 和 Pagerank
    documents, titles, anchor_texts, urls = process_results(
        {"hits": {"hits": merged_results}}
    )

    vectorizer_content = TfidfVectorizer(tokenizer=tokenize)
    vectorizer_title = TfidfVectorizer(tokenizer=tokenize)
    vectorizer_anchor = TfidfVectorizer(tokenizer=tokenize)

    tfidf_matrix_content = vectorizer_content.fit_transform(documents)
    tfidf_matrix_title = vectorizer_title.fit_transform(titles)
    tfidf_matrix_anchor = vectorizer_anchor.fit_transform(anchor_texts)

    query_vector_content = vectorizer_content.transform([" ".join(tokenize(query))])
    query_vector_title = vectorizer_title.transform([" ".join(tokenize(query))])
    query_vector_anchor = vectorizer_anchor.transform([" ".join(tokenize(query))])

    cosine_similarities_content = cosine_similarity(
        query_vector_content, tfidf_matrix_content
    ).flatten()
    cosine_similarities_title = cosine_similarity(
        query_vector_title, tfidf_matrix_title
    ).flatten()
    cosine_similarities_anchor = cosine_similarity(
        query_vector_anchor, tfidf_matrix_anchor
    ).flatten()

    pagerank_values = get_pagerank(urls)

    scores = []
    for i, url in enumerate(urls):
        combined_score = (
            cosine_similarities_title[i]
            + cosine_similarities_content[i] * 0.1
            + cosine_similarities_anchor[i] * 0.1
            + pagerank_values[url] * 0.1
        )
        scores.append((url, combined_score))

    # 按得分排序
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)

    return sorted_scores


if __name__ == "__main__":
    query = input("请输入查询字符串: ")
    results = search_and_rank(query)
    for url, score in results:
        print(f"URL: {url}, Score: {score}")
