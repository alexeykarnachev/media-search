import datetime

from mediasearch.esapi.vk import EsApiVk
from mediasearch.utilities import do_in_window, get_sec_from_str
import pandas as pd
import matplotlib.pyplot as plt

if __name__ == '__main__':
    window_str = '28d'
    step_str = '1h'
    es_api = EsApiVk([{'host': 'localhost', 'port': 9200}])

    page = es_api.es.search(
        index='vk',
        sort='date:asc',
        doc_type='wall_post',
        scroll='2m',
        size=1000,
        body={"query": {
            "query_string": {
                'default_field': 'text',
                "query": "РПЦ"
            }
        }})
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    dates = []
    while scroll_size > 0:
        hits = page['hits']['hits']
        for hit in hits:
            score = hit['_score']
            text = hit['_source']['text']
            date = hit['_source']['date']
            dates.append(date)
            print(text)

        page = es_api.es.scroll(scroll_id=sid, scroll='2m')
        sid = page['_scroll_id']
        scroll_size = len(hits)

    x_, y_ = do_in_window(x=dates, window=get_sec_from_str(window_str), step=get_sec_from_str(step_str), func=len)

    page = es_api.es.search(
        index='vk',
        sort='date:asc',
        doc_type='wall_post',
        scroll='2m',
        size=1000,
        body={"query": {
            "range": {
                "date": {'gte': dates[0], 'lte': dates[-1]}
            }
        }})

    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    dates = []
    while scroll_size > 0:
        hits = page['hits']['hits']
        for hit in hits:
            score = hit['_score']
            text = hit['_source']['text']
            date = hit['_source']['date']
            dates.append(date)
            # print(score)

        page = es_api.es.scroll(scroll_id=sid, scroll='2m')
        sid = page['_scroll_id']
        scroll_size = len(hits)

    x_all, y_all = do_in_window(x=dates, window=get_sec_from_str(window_str), step=get_sec_from_str(step_str), func=len)

    res = pd.Series(y_ / y_all, index=map(datetime.datetime.fromtimestamp, x_all))
    res.plot()
    plt.show()
