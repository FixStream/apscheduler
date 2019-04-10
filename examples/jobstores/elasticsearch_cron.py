from subprocess import call

import pytz
import time
import os
from elasticsearch import Elasticsearch
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format = '%(asctime)s,%(msecs)d %(levelname)-8s [%(pathname)s:%(filename)s:%(lineno)d] %(message)s',

    handlers=[
        logging.StreamHandler()
    ])


def cron_job():
    print("In cron job")


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    host_url = 'http://172.16.205.137:9200/'

    es = Elasticsearch([host_url])
    scheduler.add_jobstore('elasticsearch_js', doc_type='jobs', client=es)
    # scheduler.configure(timezone=utc)
    scheduler.add_job(cron_job, 'cron', second=10, start_date=datetime.now(), timezone=pytz.UTC)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()