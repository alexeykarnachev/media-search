import numpy as np


def do_in_window(x, window, step, func):
    x = np.array(x)
    left_value = x[0]
    n = ((x[-1] - x[0] - window) // step) + 1
    x_res = np.zeros(n)
    y_res = np.zeros(n)
    i = 0
    while True:
        right_value = left_value + window
        if right_value > x[-1]:
            break
        else:
            x_ = x[(x >= left_value) & (x <= right_value)]
            x_res[i] = left_value
            y_res[i] = func(x_)
            left_value += step
            i += 1
    return np.array(x_res), np.array(y_res)


def get_sec_from_str(x):
    d = str(x[-1])
    t = int(x[:-1])
    if d == 's':
        t *= 1
    elif d == 'm':
        t *= 60
    elif d == 'h':
        t *= 3600
    elif d == 'd':
        t *= 86400
    elif d == 'w':
        t *= 604800
    else:
        raise ValueError('Lexema {} is not recognized'.format(d))

    return int(t)


def get_scroll_search_hits(es, index, doc_type, body, sort=None):
    page = es.search(
        index=index,
        sort=sort,
        doc_type=doc_type,
        scroll='2m',
        size=1000,
        body=body)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']

    res_hits = []
    while scroll_size > 0:
        hits = page['hits']['hits']
        res_hits += hits
        page = es.scroll(scroll_id=sid, scroll='2m')
        sid = page['_scroll_id']
        scroll_size = len(hits)

    return res_hits
