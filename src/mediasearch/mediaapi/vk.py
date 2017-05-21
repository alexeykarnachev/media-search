import logging
from time import sleep

import vk
from requests.exceptions import ReadTimeout, HTTPError, ConnectTimeout, ConnectionError
from vk.exceptions import VkAPIError

log_level = logging.DEBUG
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def handle_exceptions(f):
    def wrapper(self, *args, **kwargs):
        if self.session is None:
            self.start_session()

        res = None
        success = False
        while not success:
            try:
                res = f(self, *args, **kwargs)
                success = True
            except (ReadTimeout, VkAPIError, HTTPError, ConnectTimeout, ConnectionError) as e:
                logger.error('Error: {}'.format(str(e)))
                if hasattr(e, 'error_data') and e.error_data.get('error_code', 0) == 15:
                    res = None
                    success = True
                else:
                    logger.info('Waiting for {} seconds and repeat'.format(MediaApiVk.TIMEOUT_SLEEP))
                    sleep(MediaApiVk.TIMEOUT_SLEEP)

        sleep(MediaApiVk.REQUEST_SLEEP)
        return res

    return wrapper


class MediaApiVk:
    REQUEST_SLEEP = 0.3
    TIMEOUT_SLEEP = 1
    MAX_COUNT = 100

    def __init__(self, token):
        self.token = token
        self.session = None
        self.api = None

    def start_session(self):
        self.session = vk.Session(access_token=self.token)
        self.api = vk.API(self.session)

    @handle_exceptions
    def get_wall_posts(self, owner_id, count=1, offset=0):
        res = self.api.wall.get(owner_id=owner_id, count=count, offset=offset)
        return res

    @handle_exceptions
    def get_wall_post_comments(self, wall_post_from_id, wall_post_id, count=1, offset=0):
        res = self.api.wall.getComments(owner_id=wall_post_from_id, post_id=wall_post_id, count=count, offset=offset,
                                        need_likes=True)
        for i in range(1, len(res)):
            res[i]['wall_post_from_id'] = wall_post_from_id
            res[i]['wall_post_id'] = wall_post_id
        return res
