from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import pickle
import pitchfork
import numpy as np
from multiprocessing import Pool
import time


with open ('urls', 'rb') as fp:
    urls = pickle.load(fp)


print(len(urls))

#output = np.empty([0, 5])
#i = 0


def f(url) :
    add = []
    add.append(url)
    request = requests.get(url)
    if request.status_code == 503:
        tim.sleep(2)

    soup = BeautifulSoup(request.content, "html.parser")

    sc = soup.find(class_='score')
    score = sc.string


    add.append(score)
    al = soup.find(class_='single-album-tombstone__review-title')
    album = al.string
    add.append(album)
    ar = soup.find(class_='artist-links artist-list single-album-tombstone__artist-links')
    artist = ar.string
    add.append(artist)
    yr = soup.find(class_='single-album-tombstone__meta-year')
    year = str(yr.text)
    word = year.split()
    try :
        real_year = word[1]
    except IndexError:
        real_year = 'null'
    add.append(real_year)
    #output = np.vstack((output,add))

    print('###############################')
    print(add)
    print('###############################')
    #print(i)
    # print('###############################')
    with open('pitchfork_data', 'wb') as fp:
        pickle.dump(add, fp)
    return add

if __name__ == "__main__":
    with Pool (42) as p:
        output = p.map(f, urls)
        with open('pitchfork_data', 'wb') as fp:
           pickle.dump(output, fp)
