
import os
import sys
from datetime import datetime, timedelta


from elasticsearch import Elasticsearch
from apscheduler.schedulers.blocking import BlockingScheduler

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format = '%(asctime)s,%(msecs)d %(levelname)-8s [%(pathname)s:%(filename)s:%(lineno)d] %(message)s',

    handlers=[
        logging.StreamHandler()
    ])


def alarm(time):

    print('Alarm! This alarm was scheduled att %s.' % time)


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    host_url = 'http://172.16.205.137:9200/'

    es = Elasticsearch([host_url])
    scheduler.add_jobstore('elasticsearch_js', doc_type='jobs', client=es)

    if len(sys.argv) > 1 and sys.argv[1] == '--clear':
        scheduler.remove_all_jobs()

    alarm_time = datetime.now() + timedelta(seconds=10)

    scheduler.add_job(alarm, 'date', run_date=alarm_time, args=[str(datetime.now())])

    print('To clear the alarms, run this example with the --clear argument.')
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
