# 这个文件用于爬取网页数据并将url和html路径存储在webpages.csv文件中

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import os
import csv
import uuid


class WebCrawler:
    def __init__(self, start_url, max_pages, save_dir):
        self.start_url = start_url
        self.max_pages = max_pages
        self.visited_urls = set()
        self.to_visit_urls = [start_url]
        self.save_dir = save_dir
        self.filenum = 0
        self.csv_file = os.path.join(os.path.dirname(save_dir), "webpages.csv")
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        with open(self.csv_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["URL", "Filename"])

    def crawl(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        while self.to_visit_urls and len(self.visited_urls) < self.max_pages:
            current_url = self.to_visit_urls.pop(0)
            if current_url in self.visited_urls:
                continue

            try:
                response = requests.get(current_url, headers=headers, timeout=(2, 5))
                if response.status_code == 200:
                    response.encoding = response.apparent_encoding
                    if response.encoding == None:
                        continue
                    try:
                        html_content = response.content.decode(
                            response.encoding, errors="replace"
                        )
                    except (UnicodeDecodeError, AttributeError):
                        pass
                    self.visited_urls.add(current_url)
                    print(f"Crawled: {current_url} (Total: {len(self.visited_urls)})")
                    self.save_page(html_content, current_url)
                    self.extract_links(html_content, current_url)
                else:
                    print(f"Failed to retrieve: {current_url}")
            except requests.exceptions.Timeout:
                pass
            except requests.RequestException:
                pass
            except Exception:
                pass
        print(f"Total pages crawled: {len(self.visited_urls)}")

    def extract_links(self, html, base_url):
        try:
            soup = BeautifulSoup(html, "lxml")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if any(char in href for char in [" ", "：", "#", "javascript:"]):
                    continue
                try:
                    url = urljoin(base_url, href)
                except ValueError:
                    pass
                if (
                    "nankai.edu.cn" in url
                    and url not in self.visited_urls
                    and url not in self.to_visit_urls
                ):
                    self.to_visit_urls.append(url)
        except Exception:
            pass

    def save_page(self, html, url):
        try:
            filename = os.path.join(
                self.save_dir,
                str(self.filenum) + ".html",
            )
            self.filenum += 1
            with open(filename, "w", encoding="utf-8") as file:
                file.write(html)
            print(f"Saved: {url}")
            self.write_to_csv(url, filename)
        except OSError:
            pass
        except Exception:
            pass

    def write_to_csv(self, url, filename):
        try:
            with open(self.csv_file, mode="a", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                print(f"Writing to CSV: {url}, {filename}")
                writer.writerow([url, filename])
        except OSError:
            pass
        except Exception:
            pass


if __name__ == "__main__":
    start_url = "https://www.nankai.edu.cn/"
    max_pages = 100500
    save_dir = "d:\\test\ir\project\\data"

    crawler = WebCrawler(start_url, max_pages, save_dir)
    crawler.crawl()
