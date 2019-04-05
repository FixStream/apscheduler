from __future__ import absolute_import
import warnings

from pytz import timezone
from elasticsearch import Elasticsearch, helpers

from apscheduler.job import Job
from apscheduler.triggers.date import DateTrigger
from apscheduler.util import (maybe_ref, datetime_repr, str_to_datetime, utc_timestamp_to_datetime,
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
        # self.collection.ensure_index('next_run_time', sparse=True)

    def create_index(self, index_name):

        try:
            response = self.client.indices.create(index=index_name)
        except RequestError as re:
            raise re
        return response

    def check_index_exists(self, index_name):

        response = self.client.indices.exists(index=index_name)

        return response

    def fetch_by_id(self, id):

        try:
            self.client.get(index=self.index, doc_type=self.doc_type, id=id)
        except NotFoundError:
            return False

        return True

    def add_job(self, job):

        if self.fetch_by_id(job.id):
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

        self.client.index(index=self.index, doc_type=self.doc_type, body=job_body, id=job_body['id'])

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        # return self._get_jobs({'next_run_time': {'$lte': timestamp}})
        due_jobs = self._get_jobs({})
        return due_jobs

    def get_all_jobs(self):
        jobs = self._get_jobs({})
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def _get_jobs(self, conditions):
        jobs = []
        failed_job_ids = []

        response = self.fetch_all(self.index, self.doc_type)
        total_jobs = self.data_cleansing(response)


        for each_job in total_jobs:

            job_state = each_job['_source']['job_state']
            job_state['next_run_time'] = utc_timestamp_to_datetime(job_state['next_run_time'])
            trigger = job_state['trigger']
            run_date = str_to_datetime(trigger['run_date'])
            time_zone = timezone(trigger['timezone'])
            dt = DateTrigger(run_date, time_zone)
            job_state['trigger'] = dt

            try:
                job_obj = self._reconstitute_job(job_state)
                jobs.append(job_obj)
            except BaseException as be:
                self._logger.exception('Unable to restore job "%s" -- removing it',
                                       job_state['id'])
                failed_job_ids.append(job_state['id'])

        if failed_job_ids:
            pass

        return jobs

    def _reconstitute_job(self, job_state):
        job = Job.__new__(Job)
        job.__setstate__(job_state)
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        return job

    def get_next_run_time(self):
        pass

    def lookup_job(self, job_id):
        pass

    def remove_all_jobs(self):
        pass

    def remove_job(self, job_id):
        pass

    def update_job(self, job):
        pass


    def fetch_all(self, index, doc_type):
        res = self.client.search(index=index, doc_type=doc_type, body={
            'size': 1000,
            'query': {
                'match_all': {}
            },
            'sort': [{ 'next_run_time': {'order' : 'asc'}}]

        })

        return res

    def data_cleansing(self, data):

        hits = data.get('hits', {}).get('hits')

        if hits:
            return hits

