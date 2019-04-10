from __future__ import absolute_import
import warnings

from pytz import timezone
from elasticsearch import Elasticsearch

from apscheduler.job import Job
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import (maybe_ref, datetime_repr, str_to_datetime,
                              utc_timestamp_to_datetime,
                              datetime_to_utc_timestamp)
from apscheduler.jobstores.base import BaseJobStore, ConflictingIdError


try:
    import cPickle as pickle
except ImportError:  # pragma: nocover
    import pickle

try:
    from elasticsearch.exceptions import NotFoundError, RequestError
except ImportError:  # pragma: nocover
    raise ImportError('ElasticsearchJobStore requires elasticsearch installed')


class ElasticsearchJobStore(BaseJobStore):

    def __init__(self, database='apscheduler', doc_type='jobs', client=None,
                 pickle_protocol=pickle.HIGHEST_PROTOCOL, **connect_args):
        super(ElasticsearchJobStore, self).__init__()
        self.pickle_protocol = pickle_protocol

        if not database:
            raise ValueError('The "database" parameter must not be empty')
        if not doc_type:
            raise ValueError('The "doc_type" parameter must not be empty')

        if client:
            self.client = maybe_ref(client)
        else:
            connect_args.setdefault('w', 1)
            self.client = Elasticsearch(**connect_args)

        self.index = database
        self.doc_type = doc_type

        if not self.check_index_exists(self.index):
            self.create_index(self.index)

    def start(self, scheduler, alias):
        super(ElasticsearchJobStore, self).start(scheduler, alias)

    def create_index(self, index_name):

        try:
            response = self.client.indices.create(index=index_name)
        except RequestError as re:
            raise re
        return response

    def check_index_exists(self, index_name):

        response = self.client.indices.exists(index=index_name)

        return response

    def add_job(self, job):

        if self.fetch_by_id(self.index, self.doc_type, job.id):
            raise ConflictingIdError(job.id)

        trigger_type = None

        job_obj = job.__getstate__()
        job_obj['next_run_time'] = datetime_to_utc_timestamp(job.next_run_time)

        if isinstance(job.trigger, DateTrigger):
            timezone = str(job_obj['trigger']._timezone)
            run_date = datetime_repr(job_obj['trigger']._run_date)
            job_obj['trigger'] = {'run_date': run_date, 'timezone': timezone}
            trigger_type = 'DateTrigger'

        elif isinstance(job.trigger, IntervalTrigger):
            init_params = job_obj['trigger'].init_params
            init_params['start_date'] = datetime_repr(init_params['start_date']) if init_params['start_date'] else None
            init_params['end_date'] = datetime_repr(init_params['end_date']) if init_params['end_date'] else None
            init_params['timezone'] = str(init_params['timezone']) if init_params['timezone'] else None
            job_obj['trigger'] = init_params
            trigger_type = 'IntervalTrigger'

        elif isinstance(job.trigger, CronTrigger):
            init_params = job_obj['trigger'].init_params
            init_params['start_date'] = datetime_repr(init_params['start_date']) if init_params['start_date'] else None
            init_params['end_date'] = datetime_repr(init_params['end_date']) if init_params['end_date'] else None
            init_params['timezone'] = str(init_params['timezone']) if init_params['timezone'] else None
            job_obj['trigger'] = init_params
            trigger_type = 'CronTrigger'

        job_body = {
            'id': job.id,
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': job_obj,
            'trigger_type': trigger_type
        }

        self.client.index(index=self.index, doc_type=self.doc_type, body=job_body,
                          id=job_body['id'])

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        _condition = {
            "query": {
                "range": {
                    "next_run_time": {
                        "lte": timestamp
                    }
                }
            }
        }
        due_jobs = self._get_jobs(_condition)
        return due_jobs

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def _get_jobs(self, condition=None):
        jobs = []
        failed_job_ids = []

        if condition:
            response = self.fetch_by_condition(self.index, self.doc_type, condition)
        else:
            response = self.fetch_all(self.index, self.doc_type)

        hits = self.data_cleansing(response)
        total_jobs = hits if hits else []

        for each_job in total_jobs:

            job_state = each_job['_source']['job_state']
            trigger_type = each_job['_source']['trigger_type']

            try:
                job_obj = self.create_trigger_obj(job_state, trigger_type)
                jobs.append(job_obj)
            except BaseException as be:
                self._logger.exception('Unable to restore job "%s" -- removing it',
                                       job_state['id'])
                failed_job_ids.append(job_state['id'])

        if failed_job_ids:
            for job_id in failed_job_ids:
                self.delete_by_id(self.index, self.doc_type, job_id)

        return jobs

    def create_trigger_obj(self, job_state, trigger_type):

        trigger = job_state['trigger']
        job_state['next_run_time'] = utc_timestamp_to_datetime(job_state['next_run_time'])

        if trigger_type == 'DateTrigger':
            time_zone = timezone(trigger['timezone'])
            run_date = str_to_datetime(trigger['run_date'])
            trigger_obj = DateTrigger(run_date, time_zone)

        elif trigger_type == 'IntervalTrigger':
            trigger['start_date'] = str_to_datetime(trigger['start_date']) if trigger['start_date'] else None
            trigger['end_date'] = str_to_datetime(trigger['end_date']) if trigger['end_date'] else None
            trigger['timezone'] = timezone(trigger['timezone']) if trigger['timezone'] else None

            trigger_obj = IntervalTrigger(weeks=trigger['weeks'], days=trigger['days'],
                                 hours=trigger['hours'], minutes=trigger['minutes'],
                                 seconds=trigger['seconds'], start_date=trigger['start_date'],
                                 end_date=trigger['end_date'], timezone=trigger['timezone'],
                                 jitter=trigger['jitter'])

        elif trigger_type == 'CronTrigger':
            trigger['start_date'] = str_to_datetime(trigger['start_date']) if trigger['start_date'] else None
            trigger['end_date'] = str_to_datetime(trigger['end_date']) if trigger['end_date'] else None
            trigger['timezone'] = timezone(trigger['timezone']) if trigger['timezone'] else None

            trigger_obj = CronTrigger(year=trigger['year'], month=trigger['month'],
                                      day=trigger['day'], week=trigger['week'],
                                      day_of_week=trigger['day_of_week'], hour=trigger['hour'],
                                      minute=trigger['minute'], second=trigger['second'],
                                      start_date=trigger['start_date'], end_date=trigger['end_date'],
                                      timezone=trigger['timezone'], jitter=trigger['jitter'])

        job_state['trigger'] = trigger_obj

        job_obj = self._reconstitute_job(job_state)

        return job_obj

    def _reconstitute_job(self, job_state):
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def get_next_run_time(self):
        next_run_time = None
        _condition = {
            "query": {
                "range": {
                    "next_run_time": {
                        "gt": 0
                    }
                }
            }
        }
        response = self.fetch_by_condition(self.index, self.doc_type, _condition, size=1)
        if response:
            hits = self.data_cleansing(response)
            if hits:
                next_run_time = hits[0]['_source']['next_run_time']

        return utc_timestamp_to_datetime(next_run_time) if next_run_time else None

    def lookup_job(self, job_id):
        response = self.fetch_by_id(self.index, self.doc_type, job_id)
        if response:
            return self.create_trigger_obj(response['job_state'], response['trigger_type'])

    def remove_all_jobs(self):
        self.delete_all(self.index, self.doc_type)

    def remove_job(self, job_id):
        self.delete_by_id(self.index, self.doc_type, job_id)

    def update_job(self, job):
        job_obj = job.__getstate__()
        job_obj['next_run_time'] = datetime_to_utc_timestamp(job.next_run_time)

        run_date = datetime_repr(job_obj['trigger']._run_date)
        timezone = str(job_obj['trigger']._timezone)
        job_obj['trigger'] = {'run_date': run_date, 'timezone': timezone}

        job_body = {
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': job_obj
        }

        self.client.update(index=self.index, doc_type=self.doc_type,
                           id=job.id, body=job_body)


    def fetch_all(self, index, doc_type, size=1000):
        _body = {
            'size': size,
            'query': {
                'match_all': {}
            },
            'sort': [
                {'next_run_time':
                    {
                        'order': 'asc'
                    }
                }
            ]

        }
        res = self.client.search(index=index, doc_type=doc_type, body=_body)

        return res

    def fetch_by_id(self, index, doc_type, id):
        try:
            response = self.client.get(index=index, doc_type=doc_type, id=id)
        except NotFoundError:
            return

        if '_source' in response:
            return response['_source']

        return

    def fetch_by_condition(self, index, doc_type,  condition, size=1000):
        _body = {
            'size': size,
            'sort': [
                {'next_run_time':
                    {
                        'order': 'asc'
                    }
                }
            ]

        }
        _body.update(condition)

        res = self.client.search(index=index, doc_type=doc_type, body=_body)

        return res

    def delete_all(self, index, doc_type):
        _body = {
          "query": {
            "match_all": {}
          }
        }

        self.client.delete_by_query(index, _body, doc_type)

    def delete_by_id(self, index, doc_type, id):

        self.client.delete(index, doc_type, id)

    def data_cleansing(self, data):

        hits = data.get('hits', {}).get('hits')

        if hits:
            return hits

