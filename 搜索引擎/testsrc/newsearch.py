from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from elasticsearch import Elasticsearch

# 初始化 Elasticsearch 客户端
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])


def tokenize(text):
    """使用 IK 分词器对文本进行分词。"""
    response = es.indices.analyze(
        index="web_pages_test", body={"text": text, "analyzer": "ik_max_word"}
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
                    "content": {
                        "value": query_text,
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


def search_and_rank(query):
    """处理查询并对结果进行排序的主搜索函数。"""
    print(f"Original query: {query}")

    # 第一步：检查查询是否为 URL
    if is_url(query):
        url_response = search_url(query)
        if url_response["hits"]["hits"]:
            print("这是一个 URL 查询")
            return [
                (hit["_source"]["url"], 1.0) for hit in url_response["hits"]["hits"]
            ]

    # 第二步：检查查询是否包含通配符
    if "*" in query or "?" in query:
        print("这是一个通配符查询")
        wildcard_response = search_wildcard(query)
        if wildcard_response["hits"]["hits"]:
            documents, titles, anchor_texts, urls = process_results(wildcard_response)
        else:
            return []  # 如果没有匹配的通配符查询结果，返回空列表

    else:
        # 第三步：对查询进行分词以进行其他类型的搜索
        tokens = tokenize(query)
        query_text = " ".join(tokens)
        print(f"Tokenized query: {query_text}")

        # 第四步：尝试精确短语匹配
        phrase_response = search_phrase(query)
        if phrase_response["hits"]["hits"]:
            print("这是一个精确短语查询")
            documents, titles, anchor_texts, urls = process_results(phrase_response)
        else:
            # 第五步：尝试多词匹配（如果未找到精确短语匹配）
            match_response = search_match(query_text)
            if match_response["hits"]["hits"]:
                print("这是一个多词查询")
                documents, titles, anchor_texts, urls = process_results(match_response)
            else:
                return []  # 如果没有匹配的结果，返回空列表

    # 第六步：计算 TF-IDF 矩阵
    vectorizer_content = TfidfVectorizer(tokenizer=tokenize)
    vectorizer_title = TfidfVectorizer(tokenizer=tokenize)
    vectorizer_anchor = TfidfVectorizer(tokenizer=tokenize)

    tfidf_matrix_content = vectorizer_content.fit_transform(documents)
    tfidf_matrix_title = vectorizer_title.fit_transform(titles)
    tfidf_matrix_anchor = vectorizer_anchor.fit_transform(anchor_texts)

    query_vector_content = vectorizer_content.transform([query_text])
    query_vector_title = vectorizer_title.transform([query_text])
    query_vector_anchor = vectorizer_anchor.transform([query_text])

    cosine_similarities_content = cosine_similarity(
        query_vector_content, tfidf_matrix_content
    ).flatten()
    cosine_similarities_title = cosine_similarity(
        query_vector_title, tfidf_matrix_title
    ).flatten()
    cosine_similarities_anchor = cosine_similarity(
        query_vector_anchor, tfidf_matrix_anchor
    ).flatten()

    # 第七步：获取 PageRank 值
    pagerank_values = get_pagerank(urls)

    # 第八步：计算综合得分
    scores = []
    for i, url in enumerate(urls):
        combined_score = (
            cosine_similarities_title[i]
            + cosine_similarities_content[i] * 0.1
            + cosine_similarities_anchor[i] * 0.1
            + pagerank_values[url] * 0.1
        )
        scores.append((url, combined_score))

    # 第九步：按得分排序
    sorted_scores = sorted(scores, key=lambda x: x[1], reverse=True)

    return sorted_scores


# 示例用法
query = "网络空间*"
results = search_and_rank(query)
for url, score in results:
    print(f"URL: {url}, Score: {score}")
