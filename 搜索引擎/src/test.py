# import search as s

import newsearch as s

# Test the search functions
query = input("Enter a search query: ")
college = "计算机学院"
results = s.search_and_rank(query, college)
print("-------------------------------查询结果如下---------------------------------")
for url, score in results[:30]:
    print(f"URL: {url}, title: {score}")
