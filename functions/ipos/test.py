import requests
data = [
    {'url': 'lkdk-2323', 'name': 's'},
    {'url': 'jdsdkjf02323', 'name': 'cv'}
]

# params = {'message': 'clover-health-ipo--69e00017'}
r = requests.post(url='http://127.0.0.1:8080', json=data)
