import logging

import yaml

from mediasearch.scraperapi.vk import ScraperVk
from mediasearch.utilities import get_sec_from_str

log_level = logging.DEBUG
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

with open('../config/get_new_wall_post_comments.conf', 'r') as f:
    conf = yaml.load(f)

vk_token = conf['vk_token']
es_hosts = conf['es_hosts']
walls = conf['walls']
post_date_lag = get_sec_from_str(conf['post_date_lag_str'])
post_date_range = get_sec_from_str(conf['post_date_range_str'])

if __name__ == '__main__':
    scraper = ScraperVk(vk_token=vk_token, es_hosts=es_hosts)

    for wall_name in walls:
        logger.info('Parsing wall comments: {} with id: {}'.format(wall_name, walls[wall_name]))
        wall_post_from_id = walls[wall_name]
        scraper.update_comments(post_date_lag=post_date_lag, post_date_range=post_date_range,
                                wall_post_from_id=wall_post_from_id, skip_if_any_comment_exists=True)
