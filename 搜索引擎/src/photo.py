# 这个模块用于从网址中下载所有的资源文件并修改资源路径

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
from elasticsearch import Elasticsearch, helpers


# 获取网页内容和资源
def download_webpage(url):
    # 请求网页 HTML
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers, timeout=(4, 8))
    response.encoding = response.apparent_encoding  # 使用响应的推测编码
    soup = BeautifulSoup(response.text, "html.parser")

    # 创建一个目录来存储网页快照
    domain = urlparse(url).netloc
    snapshot_dir = os.path.join("d:\\test\\ir\\project\\photos", f"{domain}_snapshot")
    if not os.path.exists(snapshot_dir):
        os.makedirs(snapshot_dir)

    # 保存 HTML 内容
    html_file = os.path.join(snapshot_dir, "index.html")
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(soup.prettify())

    # 下载页面中的所有资源（图片、CSS、JS等）
    resources = soup.find_all(["img", "link", "script"])
    for resource in resources:
        if resource.name == "img" and resource.get("src"):
            resource_url = resource["src"]
            if not resource_url.startswith(("http", "https")):
                resource_url = urljoin(url, resource_url)
            resource_path = download_resource(resource_url, snapshot_dir)
            resource["src"] = os.path.relpath(resource_path, start=snapshot_dir)
        elif resource.name == "link" and resource.get("href"):
            resource_url = resource["href"]
            if not resource_url.startswith(("http", "https")):
                resource_url = urljoin(url, resource_url)
            resource_path = download_resource(resource_url, snapshot_dir)
            resource["href"] = os.path.relpath(resource_path, start=snapshot_dir)
        elif resource.name == "script" and resource.get("src"):
            resource_url = resource["src"]
            if not resource_url.startswith(("http", "https")):
                resource_url = urljoin(url, resource_url)
            resource_path = download_resource(resource_url, snapshot_dir)
            resource["src"] = os.path.relpath(resource_path, start=snapshot_dir)

    # 保存更新后的 HTML 内容
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(soup.prettify())

    return html_file


# 下载资源
def download_resource(resource_url, snapshot_dir):
    # 获取资源文件名
    resource_name = os.path.basename(urlparse(resource_url).path)
    resource_path = os.path.join(snapshot_dir, resource_name)

    if not os.path.exists(snapshot_dir):
        os.makedirs(snapshot_dir)

    # 下载并保存资源
    with requests.get(resource_url, stream=True) as r:
        with open(resource_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded: {resource_url} to {resource_path}")
    return resource_path


# 示例：下载网页及其资源

# 从csv文件中读取所有的url
es = Elasticsearch([{"host": "localhost", "port": 9200, "scheme": "http"}])

index_name = "photo_in"

index_settings = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {"url": {"type": "keyword"}, "filepath": {"type": "keyword"}}
    },
}

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
es.indices.create(index=index_name, body=index_settings)

csv_file_path = "D:\\test\ir\project\webpages.csv"
actions = []
with open(csv_file_path, "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    i = 0
    for row in reader:
        url = row["URL"]
        try:
            filepath = download_webpage(url)
        except:
            print("An error!")
            continue
        action = {
            "_index": index_name,
            "_source": {
                "url": url,
                "filepath": filepath,
            },
        }
        actions.append(action)
        i += 1
        if i > 1000:
            break
helpers.bulk(es, actions, chunk_size=100, request_timeout=120)
