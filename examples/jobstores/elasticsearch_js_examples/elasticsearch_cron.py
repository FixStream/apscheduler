
from datetime import datetime
from es_background_scheduler import scheduler, cron_job


scheduler.add_job(cron_job, 'cron', second=10, start_date=datetime.now())
