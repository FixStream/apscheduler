# import sys
# sys.path.append('/home/tan/fs_workspace/dc/fs_aps/aps1/apscheduler/examples/jobstores')

import os
import logging

from elasticsearch import Elasticsearch
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(
    level=logging.DEBUG,
    format = '%(asctime)s,%(msecs)d %(levelname)-8s [%(pathname)s:%(filename)s:%(lineno)d] %(message)s',

    handlers=[
        logging.StreamHandler()
    ])


def date_job(args):
    print('Executing Date job.....................: {}'.format(args))


def interval_job():
    print("Executing Interval job.................")


def cron_job():
    print('Executing Cron job.....................')


scheduler = BackgroundScheduler()
host_url = 'http://172.16.205.137:9200/'

es = Elasticsearch([host_url])
scheduler.add_jobstore('elasticsearch_js', doc_type='jobs', client=es)
scheduler.start()


if __name__ == '__main__':

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            # time.sleep(10)
            pass
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()