from cluster_io import *

if __name__ == "__main__":
    data1 = {
      "request_id": "gatewayUID.1",
      "gateway_id": "as9dyf-asdf-0rgwj",
      "action": "build_index",
      "url": "https://scholar.google.com/scholar?q=artificial+intelligence+site:ieeexplore.ieee.org&hl=en&as_sdt=0,5",
      "index_id": "1"
    }

    data2 = {
      "request_id": "gatewayUID.2",
      "gateway_id": "as9dyf-asdf-0rgwj",
      "action": "search",
      "index_id": "1",
      "word": "platform"
    }

    data3 = {
      "request_id": "gatewayUID.3",
      "gateway_id": "as9dyf-asdf-0rgwj",
      "action": "topn",
      "index_id": "1",
      "num": "10"
    }

    print(process(data1))
    print(process(data2))
    print(process(data3))
