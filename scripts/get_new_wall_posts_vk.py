import logging

import yaml

from mediasearch.scraperapi.vk import ScraperVk

log_level = logging.DEBUG
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

with open('../config/get_new_wall_posts.conf', 'r') as f:
    conf = yaml.load(f)

vk_token = conf['vk_token']
es_hosts = conf['es_hosts']
walls = conf['walls']

if __name__ == '__main__':
    scraper = ScraperVk(vk_token=vk_token, es_hosts=es_hosts)

    for wall in walls:
        logger.info('Parsing wall: {} with id: {}'.format(wall, walls[wall]))
        from_id = walls[wall]
        scraper.get_new_wall_posts(from_id=from_id)
