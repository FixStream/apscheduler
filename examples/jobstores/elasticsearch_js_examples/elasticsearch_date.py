# import sys
# sys.path.append('/home/tan/fs_workspace/dc/fs_aps/aps1/apscheduler/examples/jobstores')


from datetime import datetime, timedelta
from es_background_scheduler import scheduler, date_job


alarm_time = datetime.now() + timedelta(seconds=10)

scheduler.add_job(date_job, 'date', run_date=alarm_time, args=[str(datetime.now())])


