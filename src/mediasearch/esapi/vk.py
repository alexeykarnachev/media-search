from elasticsearch import Elasticsearch, helpers, NotFoundError

from mediasearch.utilities import get_scroll_search_hits


def handle_exceptions(f):
    def wrapper(self, *args, **kwargs):
        try:
            res = f(self, *args, **kwargs)
        except NotFoundError:
            res = None

        return res

    return wrapper


class EsApiVk:
    INDEX = 'vk'
    WALL_POST = 'wall_post'
    WALL_POST_COMMENT = 'wall_post_comment'

    def __init__(self, hosts):
        self.es = Elasticsearch(hosts)

    @staticmethod
    def __get_id(from_id, id_):
        return str(from_id) + '_' + str(id_)

    @staticmethod
    def __get_first_hit_from_res(res):
        hits = res['hits']['hits']
        if len(hits) == 0:
            return None
        else:
            return hits[0]['_source']

    @handle_exceptions
    def get_wall_post(self, from_id, id_):
        res = self.es.get(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, id=self.__get_id(from_id=from_id, id_=id_))
        return self.__get_first_hit_from_res(res)

    @handle_exceptions
    def get_wall_posts_in_range(self, from_id=None, start_time=None, end_time=None):

        body = {'query':
                    {'bool':
                         {'must': [{"range": {"date": {}}},
                                   {'match': {}}]}}}

        if start_time:
            body['query']['bool']['must'][0]['range']['date']['gte'] = start_time
        if end_time:
            body['query']['bool']['must'][0]['range']['date']['lte'] = end_time
        if from_id:
            body['query']['bool']['must'][1]['match']['from_id'] = from_id

        hits = get_scroll_search_hits(es=self.es, index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, body=body)
        return [x['_source'] for x in hits]

    @handle_exceptions
    def get_wall_post_comment(self, wall_post_from_id, id_):
        res = self.es.get(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST_COMMENT,
                          id=self.__get_id(from_id=wall_post_from_id, id_=id_))
        return self.__get_first_hit_from_res(res)

    @handle_exceptions
    def get_last_wall_post(self, from_id, is_oldest):
        body = {'query': {'bool': {'must': [{'match': {'from_id': from_id}}],
                                   'must_not': [{'match': {'is_pinned': 1}}]}}}

        sort = 'id:asc' if is_oldest else 'id:desc'

        res = self.es.search(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, sort=sort, body=body, size=1)

        return self.__get_first_hit_from_res(res)

    @handle_exceptions
    def get_last_wall_post_comment(self, wall_post_from_id, wall_post_id, is_oldest):
        body = {'query': {'bool': {'must': [{'match': {'wall_post_from_id': wall_post_from_id}},
                                            {'match': {'wall_post_id': wall_post_id}}],
                                   'must_not': [{'match': {'is_pinned': 1}}]}}}

        sort = 'cid:asc' if is_oldest else 'cid:desc'

        res = self.es.search(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST_COMMENT, sort=sort, body=body, size=1)

        return self.__get_first_hit_from_res(res)

    @handle_exceptions
    def get_wall_post_count(self, from_id):
        body = {'query': {'match': {'from_id': from_id}}} if from_id else {}
        res = self.es.count(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, body=body)
        return res['count']

    @handle_exceptions
    def get_wall_post_comment_count(self, wall_post_from_id, wall_post_id):
        body = {'query': {'bool': {'must': [{'match': {'wall_post_from_id': wall_post_from_id}},
                                            {'match': {'wall_post_id': wall_post_id}}]}
                          }
                } if wall_post_from_id and wall_post_id else {}

        res = self.es.count(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST_COMMENT, body=body)
        return res['count']

    def set_new_wall_post(self, body):
        if isinstance(body, list):
            actions = [
                {
                    "_index": EsApiVk.INDEX,
                    "_type": EsApiVk.WALL_POST,
                    "_id": self.__get_id(str(body[i]['from_id']), str(body[i]['id'])),
                    "_source": body[i]
                }
                for i in range(len(body))
            ]

            helpers.bulk(self.es, actions)
        else:
            self.es.index(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, body=body,
                          id=self.__get_id(str(body['from_id']), str(body['id'])))

    def set_new_wall_post_comment(self, body, wall_post_from_id):
        if isinstance(body, list):
            actions = [
                {
                    "_index": EsApiVk.INDEX,
                    "_type": EsApiVk.WALL_POST_COMMENT,
                    "_id": self.__get_id(str(wall_post_from_id), str(body[i]['cid'])),
                    "_source": body[i]
                }
                for i in range(len(body))
            ]

            helpers.bulk(self.es, actions)
        else:
            self.es.index(index=EsApiVk.INDEX, doc_type=EsApiVk.WALL_POST, body=body,
                          id=self.__get_id(str(wall_post_from_id), str(body['cid'])))
