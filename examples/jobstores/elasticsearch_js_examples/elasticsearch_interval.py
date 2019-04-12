
from datetime import datetime
from es_background_scheduler import scheduler, interval_job


scheduler.add_job(interval_job, 'interval', seconds=10, start_date=datetime.now())
