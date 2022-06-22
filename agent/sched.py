import os

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from tzlocal import get_localzone

from agent.collector import LogCollector
from agent.validator import LogValidator
from agent.backend_connector import get_config


load_dotenv('.env')


class LFBSched:
    """
    Class for scheduling synchronization with the backend.
    Synchronization can be triggered by:
    - a time of a day.
    - interval.
    - logs having exceeded the maximum size.

    Attributes:
        logs_source         : LogCollector
        sched               : BackgroundScheduler
        max_log_size        : int
        interval_sync_job   : Job
    """

    def __init__(self, source: LogCollector, validator: LogValidator, timezone: str = get_localzone()):
        self.logs_source, self.validator, self.sched = source, validator, BackgroundScheduler(timezone=timezone)
        self.max_log_size = 0
        self.interval_sync_job = None

        self.sched.add_job(self.state_maintenance_sync, 'interval', seconds=int(os.environ.get('STATE_CONTROL_INTERVAL')))
        self.sched.add_job(self.verify_logs, 'interval', seconds=int(os.environ.get('LOGS_CONTROL_INTERVAL')))

    def state_maintenance_sync(self) -> None:
        """
        Synchronizes the agent's configuration and uploads logs if they've exceeded the max_logs_size
        """
        self.sync_config()

        if self.logs_source.log_size() > self.max_log_size > 0:
            self.logs_source.upload_records()

    def verify_logs(self) -> None:
        for log in self.logs_source.get_logs():
            self.validator.verify_log(log)

    def sync_config(self) -> None:
        config = get_config()
        self._change_interval(config['snapshotInterval'])
        self._change_max_log_size(config['maxRecordCount'])

    def add_sync_interval(self, hours, minutes=0) -> None:
        self.sched.add_job(self.logs_source.upload_records, 'interval', minutes=minutes, hours=hours)

    def add_sync_time(self, hour, minute=None) -> None:
        self.sched.add_job(self.logs_source.upload_records, trigger='cron', hour=hour, minute=minute)

    def start(self) -> None:
        self.sync_config()
        self.sched.start()

    def stop(self) -> None:
        self.sched.shutdown()

    def _get_current_interval(self):
        return self.interval_sync_job.trigger.interval_length / 60 if self.interval_sync_job else 0

    def _change_interval(self, seconds):
        if seconds != self._get_current_interval():
            if self.interval_sync_job:
                if seconds > 0:
                    self.interval_sync_job.reschedule('interval', seconds=seconds)
                else:
                    self.interval_sync_job.remove()
                    self.interval_sync_job = None
            elif seconds > 0:
                self.interval_sync_job = self.sched.add_job(self.logs_source.upload_records, 'interval',
                                                            seconds=seconds)

    def _change_max_log_size(self, max_size):
        self.max_log_size = max_size
