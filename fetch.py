import os, requests, re, pickle
import random
import time

from serpapi import GoogleScholarSearch
from urllib.parse import urlsplit

SERP_API_KEY = os.getenv("SERP_API_KEY")
IEEE_KEY = os.getenv("IEEE_KEY")
GoogleScholarSearch.SERP_API_KEY = SERP_API_KEY
session = requests.Session()

def serp_google_scholar_search(query, start, num):
    """
    Using serp api to perform a search on Google Scholar

    :param query: query string E.g. "artificial+intelligence+site:ieeexplore"
    :param start: the offest of the first result
    :param num: number of papers per page
    :return: a dict of results
    """

    serp_search = GoogleScholarSearch({
        "q": query,
        "hl": "en",
        "start": start,
        "num": num
    })
    return serp_search.get_dict()

def fetch_abstract_crawler(url):
    """
    :param url: an IEEE Explore URL
    :return: the abstract of the paper specified by url
    """

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    resp = session.get(url, headers=headers)
    abstract = re.findall('"abstract":"(.+?)",', resp.text, re.S)
    for item in abstract:
        if item != 'true' and item != 'false':
            return item

def fetch_abstract(document_id):
    """
    Use IEEE API to fetch document's abstract
    """
    query = "https://ieeexploreapi.ieee.org/api/v1/search/articles?article_number={}&apikey={}".format(document_id, IEEE_KEY)
    resp = requests.get(query)
    print(resp)


def extract_document_number(url):
    m = re.search(r"/document/(\d+)(?=[/?#]|$)", url)
    return m.group(1) if m else None

def fetch_data(url, debug=False):
    query_string = urlsplit(url).query
    query = ""
    for item in query_string.split("&"):
        if item.startswith("q="):
            query = item[2:]

    data = dict()

    # only fetch at most 100 papers (by default num of papers on a page, 10 pages)
    for offset in range(0, 81, 20):
        raw_serp_data = serp_google_scholar_search(query, offset, 20)

        if len(raw_serp_data)==0 or raw_serp_data["search_information"]["total_results"] == 0:
            break

        if debug:
            print("Serp query: offset{}, length{}".format(offset, len(raw_serp_data["organic_results"])))

        for raw_data in raw_serp_data["organic_results"]:
            paper = dict()
            paper["title"] = raw_data["title"]
            paper["citation"] = raw_data["inline_links"]["cited_by"]["total"]
            paper["link"] = raw_data["link"]

            # fetch abstract from IEEE Xplore
            paper["abstract"] = fetch_abstract_crawler(raw_data["link"])
            # limiting request frequency
            time.sleep(random.uniform(0.8, 2))

            document_number = extract_document_number(raw_data["link"])

            if debug and paper["abstract"] == None:
                print(document_number)

            data[document_number] = paper

    return data

def format_words(input: str):
    """
    Turn all words into lower case, delete all special characters (;?., etc.)
    Split into words list

    :param input: a string
    :return: formatted words list
    """
    return re.split(r'\W+', input.lower())

def fetch_and_store(url, index_id, debug=False):
    """
    stores metadata in ./{index_id}-metadata.pkl, stores mapreduce input data in ./{index_id}
    """
    data = fetch_data(url, debug)
    print("Store MapReduce Data to {}".format(os.path.join("./", index_id)))
    if not os.path.exists(os.path.join("./", index_id)):
        os.makedirs(os.path.join("./", index_id))
    for key in data:
        if data[key]["abstract"] is not None:
            with open(os.path.join("./", "{}/{}.txt".format(index_id, key)), "w") as abstract:
                words = format_words(data[key]["abstract"])
                abstract.write("{} {}".format(key, ' '.join(words)))
                abstract.flush()

    with open(os.path.join("./", "{}-metadata.pkl".format(index_id)), "wb") as f:
        print("Store metadata to {}".format(os.path.join("./", "{}-metadata.pkl".format(index_id))))
        pickle.dump(data, f)
