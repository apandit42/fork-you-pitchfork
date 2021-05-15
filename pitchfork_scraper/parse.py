import json
import numpy as np
import pickle

output = np.empty([0, 5])
i = 0
while i < 119:

    with open('data_file___'+str(i)+'.json') as f:
        data = json.load(f)

    data_results =  data['results']['list']
    for d in data_results:
        add = []
        url = d['url']
        if len(d['artists']) > 0 :
            artist_real_new = '||'.join([x['display_name'] for x in d['artists']])
            # for artist in d['artists']:
            #     artist_real_new = artist['display_name']
        else:
            try:
                artist_real_new = d['artists'][0]['display_name']
            except IndexError:
                artist_real_new = "null -index error"
        #import ipdb; ipdb.set_trace()
        try:
            album_name = d['tombstone']['albums'][0]['album']['display_name']
        except IndexError:
            album_name = "null - index error"
        try:
            year = d['tombstone']['albums'][0]['album']['release_year']
        except IndexError:
            year = "null - index error"

        try:
            rating = d['tombstone']['albums'][0]['rating']['rating']
        except IndexError:
            rating = "null - index error"

        add.append(url)
        add.append(artist_real_new)
        add.append(album_name)
        add.append(year)
        add.append(rating)
        output = np.vstack((output,add))
        print(output)
        print("#####################")
        print("#####################")
        print("#####################")
        print(len(output))
    i += 1
    print(i)


with open('pitchfork_data', 'wb') as fp:
   pickle.dump(output, fp)
