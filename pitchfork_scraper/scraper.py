#import time
#from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import pickle

#driver = webdriver.Chrome('/Users/colinkalicki/bin/chromedriver')
#driver.get("https://pitchfork.com/reviews/albums/")
#time.sleep(2)  # Allow 2 seconds for the web page to open
#scroll_pause_time = 1 # You can set your own pause time. My laptop is a bit slow so I use 1 sec
#screen_height = driver.execute_script("return window.screen.height;")   # get the screen height of the web
#i = 1

#while True:
#    # scroll one screen height each time
#    driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))
#    i += 1
#    time.sleep(scroll_pause_time)
    # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
#    scroll_height = driver.execute_script("return document.body.scrollHeight;")
    # Break the loop when the height we need to scroll to is larger than the total scroll height
#    if (screen_height) * i > scroll_height:
#        break
n = 1
urls = []
while n<=2000 :
    URL = 'https://pitchfork.com/reviews/albums/?page=' + str(n)
    page = requests.get(URL)
    if page.status_code == 404 :
        break
####extract urls######

    soup = BeautifulSoup(page.content, "html.parser")
    for parent in soup.find_all(class_="review"):
        a_tag = parent.find("a", class_="review__link")
        base = "https://pitchfork.com/reviews/albums/"
        #import ipdb; ipdb.set_trace()
        link = a_tag.attrs['href']
        url = urljoin(base, link)
        urls.append(url)
    print(urls)
    print("################################")
    print(n)
    n += 1

with open('urls', 'wb') as fp:
    pickle.dump(urls, fp)
print("total number of URLS: %d" %len(urls))
