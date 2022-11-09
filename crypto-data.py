import datetime as dt
import time
import requests


def unix_30sec_chunk_count() -> int:
    """Returns the number of 30 seconds time blocks since 1 Jan 1970"""
    seconds = time.mktime(dt.datetime.now().timetuple())
    return round(seconds / 30)


def coincodex_historic_api_url(date: str) -> str:
    url_start = 'https://coincodex.com/api/coincodex/get_historical_snapshot'
    t = unix_30sec_chunk_count()
    url = f'{url_start}/{date}%2005:00/0/100?t={t}'
    return url


# https://coincodex.com/api/coincodex/get_historical_snapshot/2021-01-01 00:00/0/1?t=55598511

start_date = dt.date(2021, 1, 1)
end_date = dt.date(2022, 9, 16)
days = (end_date - start_date).days

results = []
for d in range(days + 1):
    date = start_date + dt.timedelta(days=d)
    time.sleep(5)

    res = None
    while not res:
        try:
            url = coincodex_historic_api_url(date.strftime('%Y-%m-%d'))
            res = requests.get(url, headers={'Accept': 'application/json'})
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            res = None
            print(f'\n    [Error {e.response.status_code}] Sleeping for 30 seconds - {url}')
            time.sleep(30)
    print(f'  [ {d+1} / {days} ] {date} completed  ', end='\r')
    results.append(res.json())

# converid = usd
# https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical?id=1&convertId=2781&timeStart=1655942400&timeEnd=1664582400
# https://api.coinmarketcap.com/data-api/v1/cryptocurrency/map

sess = requests.Session()
res = sess.get('https://api.coinmarketcap.com/data-api/v1/cryptocurrency/map')
coin_map = res.json()
coins = coin_map['data']
coins.sort(key=lambda d: d.get('rank', 100000))

start_date = dt.date(2020, 12, 20)
end_date = dt.date(2022, 9, 16)
days = (end_date - start_date).days

time_pairs_100_days = []
for i in range(days // 100 + 1):
    u_start = time.mktime((start_date + dt.timedelta(days=100 * i)).timetuple())
    u_end = min(
        time.mktime((start_date + dt.timedelta(days=100 * (i+1))).timetuple()),
        time.mktime(dt.datetime.now().timetuple())
    )
    time_pairs_100_days.append((u_start, u_end))

historic_data = []

for i, coin in enumerate(coins[:100]):
    for u_start, u_end in time_pairs_100_days:
        url_api = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/historical'
        coin_id = coin['id']
        url = f'{url_api}?id={coin_id}&convertId=2781&timeStart={u_start}&timeEnd={u_end}'
        res = sess.get(url, headers={'Accept': 'application/json'})
        historic_data.append(res.json()['data'])
    print(f' [ {i} / 100 ] {coin["name"]}                ', end='\r')


for d in historic_data:
    for qd in d['quotes']:
        qd.pop('timeOpen')
        qd.pop('timeClose')
        qd.pop('timeHigh')
        qd.pop('timeLow')

for d in historic_data:
    d['quotes'] = [qd['quote'] for qd in d['quotes']]

import json