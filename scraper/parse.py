import json
i = 0
while i < 119:

    with open('data_file___'+str(i)+'.json') as f:
        data = json.load(f)

    #data_results =  data['results']

    import ipdb; ipdb.set_trace()
    i += 1
