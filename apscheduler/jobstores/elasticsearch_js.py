from __future__ import absolute_import
import warnings

from pytz import timezone
from elasticsearch import Elasticsearch

from apscheduler.job import Job
from apscheduler.triggers.date import DateTrigger
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

        job_obj = job.__getstate__()
        job_obj['next_run_time'] = datetime_to_utc_timestamp(job.next_run_time)

        run_date = datetime_repr(job_obj['trigger']._run_date)
        timezone = str(job_obj['trigger']._timezone)
        job_obj['trigger'] = {'run_date': run_date, 'timezone': timezone}

        job_body = {
            'id': job.id,
            'next_run_time': datetime_to_utc_timestamp(job.next_run_time),
            'job_state': job_obj
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
        jobs = self._get_jobs({})
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def _get_jobs(self, conditions):
        jobs = []
        failed_job_ids = []

        if conditions:
            response = self.fetch_by_condition(self.index, self.doc_type, conditions)
        else:
            response = self.fetch_all(self.index, self.doc_type)

        total_jobs = self.data_cleansing(response)

        for each_job in total_jobs:

            job_state = each_job['_source']['job_state']

            try:
                job_obj = self.create_date_trigger_obj(job_state)
                jobs.append(job_obj)
            except BaseException as be:
                self._logger.exception('Unable to restore job "%s" -- removing it',
                                       job_state['id'])
                failed_job_ids.append(job_state['id'])

        if failed_job_ids:
            pass

        return jobs

    def create_date_trigger_obj(self, job_state):

        trigger = job_state['trigger']
        time_zone = timezone(trigger['timezone'])
        run_date = str_to_datetime(trigger['run_date'])
        job_state['next_run_time'] = utc_timestamp_to_datetime(job_state['next_run_time'])

        dt = DateTrigger(run_date, time_zone)
        job_state['trigger'] = dt

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
        return self.create_date_trigger_obj(response['job_state'])

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

