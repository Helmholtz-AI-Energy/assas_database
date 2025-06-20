import os
import sys
import datetime
import logging

os.environ['ASTEC_ROOT'] = '/root/astecV3.1.2'

from assasdb import AssasDatabaseManager

logger = logging.getLogger('assas_app')

logging.basicConfig(
    format = '%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s',
    level = logging.INFO,
    stream = sys.stdout)

now = datetime.datetime.now()
logger.info(f'Start update of archive sizes as cron job at {now}')

manager = AssasDatabaseManager()

manager.update_archive_sizes(
    number_of_archives = 10
)
manager.update_status_of_archives()
manager.update_meta_data_of_valid_archives()

now = datetime.datetime.now()
logger.info(f'Finished update of archives sizes at {now}')