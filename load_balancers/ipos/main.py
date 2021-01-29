import json
import requests


def run(data):

    urls = ['https://us-east1-cb-data-fetch.cloudfunctions.net/ipos_simple_ent_f0',
            'https://us-east4-cb-data-fetch.cloudfunctions.net/ipos_simple_ent_f1',
            'https://asia-northeast2-cb-data-fetch.cloudfunctions.net/ipos_simple_ent_f2'
            ]
    x = 0
    retry = []
    for i, c_l in enumerate(data):
        params = {'message': c_l['url']}
        print(f"params {i}: {params} ")
        r = requests.post(url=urls[x], json=params)
        if r.text == 'worked':
            print('success', c_l, i)
        else:
            print('failed url:', urls[x])
            retry.append(c_l)
        x += 1
        if x > len(urls)-1:
            x = 0

    print("Re-attempting to fetch failed urls: ", retry)
    for i, c_l in enumerate(retry):
        for url in urls:
            params = {'message': c_l}
            r = requests.post(url=url, json=params)
            if r.text == 'worked':
                print('success', c_l, i)
                break


def hello_world(request):

    if request.method == 'POST':
        request_json = request.get_json()
        # print(request_json)
        if not request_json:
            print("Server sent wrong data.")
            return 400

        run(request_json)
        return f'Hello World!'
    else:
        return 'Nothing Here'
