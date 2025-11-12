import json, os, socket
from http.client import responses
import heapq
from confluent_kafka import Consumer, Producer
from fetch import *
from hadoop import *

BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
REQUEST_TOPIC = "request"
RESPONSE_TOPIC = "response"
GROUP_ID = "cluster"
REQUEST_TYPES = ["build_index", "search", "topn"]

# ---------- Kafka helpers ----------

def create_kafka_producer() -> Producer:
    conf = {
        'bootstrap.servers': BROKER,
        'client.id': socket.gethostname()
    }
    return Producer(conf)

def create_kafka_consumer() -> Consumer:
    conf = {
        'bootstrap.servers': BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'client.id': socket.gethostname()
    }
    consumer = Consumer(conf)
    consumer.subscribe([REQUEST_TOPIC])
    return consumer


def listen(prod: Producer, cons: Consumer):
    while True:
        m = cons.poll(1.0)
        if not m:
            continue
        if m.error():
            continue
        try:
            data = json.loads(m.value().decode())
        except Exception:
            continue

        print("Received Request: {}".format(data))
        response = process(data)

        prod.produce(
            topic=RESPONSE_TOPIC,
            value=json.dumps(response).encode("utf-8"),
        )
        prod.flush(5)


def parse_inverted_index_item(item: str):
    word, value = item.split('\t', 2)
    total_count, docs = value.split(':', 2)
    doc_list = docs.split(';')
    ret = []
    for doc in doc_list:
        idx, count = doc.split(',', 2)
        ret.append((idx, count))
    ret.sort(key=lambda x:int(x[1]), reverse=True)
    return word, total_count, ret

def load_and_cache_inverted_idx(index_id):
    inverted_idx = dict()
    with open("./{}-index".format(index_id), "r") as f:
        lines = f.readlines()
    for line in lines:
        if line != "" and line != "\n":
            word, total_count, doc_list = parse_inverted_index_item(line)
            inverted_idx[word] = (total_count, doc_list)
    with open("./{}-index-parsed".format(index_id), "wb") as p:
        pickle.dump(inverted_idx, p)
    return inverted_idx

def get_inverted_idx(index_id):
    if os.path.exists("./{}-index-parsed".format(index_id)):
        with open("./{}-index-parsed".format(index_id), "rb") as f:
            return pickle.load(f)
    else:
        return load_and_cache_inverted_idx(index_id)

def get_metadata(index_id):
    with open("./{}-metadata.pkl".format(index_id), "rb") as f:
        return pickle.load(f)

def search(index_id, word):
    inverted_idx = get_inverted_idx(index_id)
    metadata = get_metadata(index_id)
    if word in inverted_idx:
        entry = inverted_idx[word]
        result = [{
            "doc_id": doc[0],
            "citations": metadata[doc[0]]["citation"],
            "title": metadata[doc[0]]["title"],
            "freq": doc[1]
        } for doc in entry[1]]
        return {
            "count": entry[0],
            "word": word,
            "result": result
        }
    else:
        return {
            "count": "0",
            "word": word,
            "result": []
        }

def topn(index_id, n):
    n = int(n)
    if n<=0 :
        return {
            "count": "0",
            "result": []
        }

    inverted_idx = get_inverted_idx(index_id)
    topn = heapq.nlargest(n, inverted_idx.items(), key=lambda x:int(x[1][0]))
    return {
        "count": "{}".format(min(n, len(inverted_idx))),
        "result": [
            {
                "word": i[0],
                "freq": i[1][0]
            } for i in topn
        ]
    }


def process(data):
    response = None
    action = data.get("action")
    index_id = data.get("index_id")

    if action == "build_index":
        url = data.get("url")
        fetch_and_store(url, index_id)
        build_index(index_id)
        code, _, _ = merge_output(index_id)
        if code != 0:
            code, _, _ = load_inverted_index_from_hdfs(index_id)
        if code == 0:
            response = {
                "request_id": data.get("request_id"),
                "action": action,
                "status": "DONE",
                "url": url,
                "index_id": index_id,
            }
        else:
            response = {
                "request_id": data.get("request_id"),
                "action": action,
                "status": "FAILED",
                "url": url,
                "index_id": index_id,
            }

    elif action == "search":
        try:
            word = data.get("word", "")
            payload = search(index_id, word)
            response = {
                "request_id": data.get("request_id"),
                "action": "search",
                "status": "DONE",
                "index_id": index_id,
                "data": payload
            }
        except Exception as e:
            response = {
                "request_id": data.get("request_id"),
                "action": "search",
                "status": "FAILED",
                "index_id": index_id,
                "error": f"internal_error: {e}"
            }

    elif action == "topn":
        try:
            try:
                n = int(data.get("num", 10))
            except Exception:
                n = 10
            payload = topn(index_id, n)
            response = {
                "request_id": data.get("request_id"),
                "action": "topn",
                "status": "DONE",
                "index_id": index_id,
                "data": payload
            }
        except Exception as e:
            response = {
                "request_id": data.get("request_id"),
                "action": "topn",
                "status": "FAILED",
                "index_id": index_id,
                "error": f"internal_error: {e}"
            }

    else:
        response = {
            "request_id": data.get("request_id"),
            "action": action,
            "status": "FAILED",
            "index_id": index_id,
            "error": f"unknown action: {action}"
        }

    return response

if __name__ == "__main__":
    prod = create_kafka_producer()
    cons = create_kafka_consumer()
    listen(prod, cons)