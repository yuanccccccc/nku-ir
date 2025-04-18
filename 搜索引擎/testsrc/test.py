import testsearch as s

# Test the search functions
query = input("Enter a search query: ")

results = s.search_and_rank(query)
for url, score in results:
    print(f"URL: {url}, Score: {score}")
