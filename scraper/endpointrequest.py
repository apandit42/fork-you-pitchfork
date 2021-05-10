from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import pickle
import pitchfork
import numpy as np
from multiprocessing import Pool
import time
import json



url = "https://pitchfork.com/api/v2/search/?types=reviews&hierarchy=sections%2Freviews%2Falbums%2Cchannels%2Freviews%2Falbums&sort=publishdate%20desc%2Cposition%20asc&size=100&start=0"
size = 200 * 73
start = size
i = 73
while i < 119 :
    url = "https://pitchfork.com/api/v2/search/?types=reviews&hierarchy=sections%2Freviews%2Falbums%2Cchannels%2Freviews%2Falbums&sort=publishdate%20desc%2Cposition%20asc&size=" + str(size) + "&start=" + str(start)
    request = requests.get(url)
    data = request.json()
    with open(f"data_file___{i}.json", "w") as write_file:
        json.dump(data, write_file)

    size += 200
    start = size
    print(i)
    print("#######################")
    print(data)
    print("#######################")
    print(url)
    print("######################")
    i += 1
print(request)


import ipdb; ipdb.set_trace()
