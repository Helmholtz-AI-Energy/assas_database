import datetime
import logging
import sys

from assasdb import AssasDatabaseManager

logger = logging.getLogger('assas_app')

logging.basicConfig(
    format = '%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s',
    level = logging.DEBUG,
    stream = sys.stdout)

now = datetime.datetime.now()
logger.info(f'Start update of archive sizes as cron job at {now}')

manager = AssasDatabaseManager()

manager.process_uploads()
manager.update_archive_sizes()

now = datetime.datetime.now()
logger.info(f'Finished update of archives sizes at {now}')