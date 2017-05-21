import logging
from time import sleep, time
from mediasearch.esapi.vk import EsApiVk
from mediasearch.mediaapi.vk import MediaApiVk

log_level = logging.DEBUG
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class ScraperVk:
    """
    Class for scraping the Vk web-site.
    It uses a vk-api and elastic search data base for storing the results.
    """

    def __init__(self, vk_token, es_hosts):
        """
        :param vk_token: vk application token (see an official vk-api doc.)
        :type vk_token: str
        :param es_hosts: es hosts (e.g. [{'host': 'localhost', 'port': 9200}])
        :type es_hosts: list
        """
        self.vk_api = MediaApiVk(vk_token)
        self.es_api = EsApiVk(es_hosts)

    def get_new_wall_posts(self, from_id):
        """
        Run this method to obtain all new posts from wall with specific id. 
        Results will be put in es db.
        :param from_id: the id of desired vk wall
        :type from_id: int
        """
        newest_row = self.es_api.get_last_wall_post(from_id=from_id, is_oldest=False)
        oldest_row = self.es_api.get_last_wall_post(from_id=from_id, is_oldest=True)

        res_start = self.vk_api.get_wall_posts(owner_id=from_id, count=1, offset=0)

        if not res_start:
            return
        n_start = res_start[0]

        n_start_orig = n_start
        db_newest_id = newest_row['id'] if newest_row else None
        db_oldest_id = oldest_row['id'] if oldest_row else None

        offset = 0
        stop = False
        n_objects_committed = 0

        while not stop:
            objects_ = []

            res = self.vk_api.get_wall_posts(owner_id=from_id, count=MediaApiVk.MAX_COUNT, offset=offset)

            n_objects_now = res[0]
            n_new_objects = n_objects_now - n_start
            res = res[1 + n_new_objects:]

            if n_new_objects > 0:
                offset += n_new_objects
                n_start = n_objects_now

            n_objects_in_db = self.es_api.get_wall_post_count(from_id=from_id)

            if n_objects_in_db == n_objects_now:
                break

            if not n_objects_in_db or n_objects_in_db < n_start_orig:
                for res_ in res:
                    pinned_object_in_db = None
                    res_id = res_['id']
                    is_pinned = res_.get('is_pinned', False)
                    if db_newest_id and db_oldest_id <= res_id <= db_newest_id:
                        if n_objects_in_db == n_objects_now:
                            stop = True
                            break
                        elif db_oldest_id <= res[-1]['id'] <= db_newest_id:
                            offset = n_objects_in_db + n_new_objects
                            break
                    else:
                        if is_pinned:
                            pinned_object_in_db = self.es_api.get_wall_post(from_id=from_id, id_=res_id)

                        if not pinned_object_in_db or not is_pinned:
                            objects_.append(res_)
                            n_objects_in_db += 1
                            offset += 1

                self.es_api.set_new_wall_post(objects_)

            else:
                stop = True

            n_objects_committed += len(objects_)
            logger.info(
                'New posts were committed. From_id id: {}, Objects committed: {} (+{})'.format(from_id,
                                                                                               n_objects_committed,
                                                                                               len(objects_)))

    def get_new_wall_post_comments(self, from_id, post_id):
        """
        Run this method to obtain all new post's comments from wall and post with specific ids. 
        Results will be put in es db.
        :param post_id: the id of desired post
        :type post_id: int
        :param from_id: the id of desired vk wall
        :type from_id: int
        """
        newest_row = self.es_api.get_last_wall_post_comment(wall_post_from_id=from_id, wall_post_id=post_id,
                                                            is_oldest=False)
        oldest_row = self.es_api.get_last_wall_post_comment(wall_post_from_id=from_id, wall_post_id=post_id,
                                                            is_oldest=True)

        res_start = self.vk_api.get_wall_post_comments(wall_post_from_id=from_id, wall_post_id=post_id,
                                                     count=1, offset=0)

        if not res_start:
            return
        n_start = res_start[0]

        n_start_orig = n_start
        db_newest_id = newest_row['cid'] if newest_row else None
        db_oldest_id = oldest_row['cid'] if oldest_row else None

        offset = 0
        stop = False
        n_objects_committed = 0

        while not stop:
            objects_ = []

            res = self.vk_api.get_wall_post_comments(wall_post_from_id=from_id, wall_post_id=post_id,
                                                     count=MediaApiVk.MAX_COUNT, offset=offset)

            n_objects_now = res[0]
            n_new_objects = n_objects_now - n_start
            res = res[1 + n_new_objects:]

            if n_new_objects > 0:
                offset += n_new_objects
                n_start = n_objects_now

            n_objects_in_db = self.es_api.get_wall_post_comment_count(wall_post_from_id=from_id, wall_post_id=post_id)

            if n_objects_in_db == n_objects_now:
                break

            if not n_objects_in_db or n_objects_in_db < n_start_orig:
                for res_ in res:
                    pinned_object_in_db = None
                    res_id = res_['cid']
                    is_pinned = res_.get('is_pinned', False)
                    if db_newest_id and db_oldest_id <= res_id <= db_newest_id:
                        if n_objects_in_db == n_objects_now:
                            stop = True
                            break
                        elif db_oldest_id <= res[-1]['cid'] <= db_newest_id:
                            offset = n_objects_in_db + n_new_objects
                            break
                    else:
                        if is_pinned:
                            pinned_object_in_db = self.es_api.get_wall_post_comment(wall_post_from_id=from_id,
                                                                                    id_=res_id)

                        if not pinned_object_in_db or not is_pinned:
                            objects_.append(res_)
                            n_objects_in_db += 1
                            offset += 1

                self.es_api.set_new_wall_post_comment(objects_, wall_post_from_id=from_id)

            else:
                stop = True

            n_objects_committed += len(objects_)
            logger.info('New comments were committed. From_id id: {}, Post_id: {}, '
                        'Objects committed: {} (+{})'.format(from_id, post_id, n_objects_committed, len(objects_)))

    def update_comments(self, post_date_lag, post_date_range, wall_post_from_id=None, skip_if_any_comment_exists=True):

        end_time = int(time())
        if post_date_lag:
            end_time -= post_date_lag

        start_time = end_time - post_date_range if post_date_range else None

        wall_posts = self.es_api.get_wall_posts_in_range(from_id=wall_post_from_id, start_time=start_time,
                                                         end_time=end_time)

        ids_tuples = [(post['from_id'], post['id']) for post in wall_posts]

        n = len(ids_tuples)
        for i in range(len(ids_tuples)):
            wall_post_from_id_, wall_post_id_ = ids_tuples[i]
            if skip_if_any_comment_exists:
                n_comments = self.es_api.get_wall_post_comment_count(wall_post_from_id=wall_post_from_id_,
                                                                     wall_post_id=wall_post_id_)
                if n_comments > 0:
                    logger.info('Skipping comments (some comments already exist) for: '
                                'Wall id: {}, Post id: {}.'.format(wall_post_from_id_, wall_post_id_))
                    continue

            self.get_new_wall_post_comments(from_id=wall_post_from_id_, post_id=wall_post_id_)
            logger.info('Post comments updated. Wall id: {}, Post: {}/{}'.format(wall_post_from_id_, i + 1, n))
