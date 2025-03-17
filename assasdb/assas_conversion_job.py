import datetime
import logging
import sys

from assasdb import AssasDatabaseManager

# TODO: Refactor this

logger = logging.getLogger('assas_app')

logging.basicConfig(
    format = '%(asctime)s %(process)d %(module)s %(levelname)s: %(message)s',
    level = logging.DEBUG,
    stream = sys.stdout)

now = datetime.datetime.now()
logger.info(f'Start conversion as cron job at {now}')

#manager = AssasDatabaseManager()

#manager.convert_next_validated_archive(
#    explicit_times = [0,10]
#)
#manager.collect_meta_data_after_conversion()

now = datetime.datetime.now()
logger.info(f'Finished conversion at {now}')