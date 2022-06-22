import threading
import time
from agent.log import Snapshot, Log
from agent.backend_connector import get_snapshots, post_log_status
from agent.lfb_agent import LfbAgent, AgentContentRequest
from agent.proxyconnection import ContentRequest
from typing import Dict, List


class _LogValidatorJob:
    def __init__(self, snapshots: List[Dict], request: AgentContentRequest, log: Log):
        self.job = request
        self.log = log
        self.remaining_snapshots = snapshots
        self.fingerprint = Snapshot.hash_fun("")
        self.records_counter = 0
        self.validation_finished = False

        if len(self.remaining_snapshots) == 0:
            self.__finish_validation(Snapshot.hash_fun(""))

    def work_in_progress(self) -> bool:
        if self.job.is_dead():
            self.__finish_validation('')
            return False

        lead = self.job.get_lead()

        return not lead or lead.get_status() in (ContentRequest.Status.PENDING, ContentRequest.Status.RECEIVING)

    def do_work(self) -> None:
        lead = self.job.get_lead()

        if lead:
            while new_record := lead.pop_record():
                self.__handle_new_record(new_record)

    def __handle_new_record(self, new_record: str) -> None:
        if not self.validation_finished:
            next_snapshot = self.remaining_snapshots[0]

            self.records_counter += 1
            self.fingerprint = Snapshot.hash_fun(self.fingerprint + new_record)

            if next_snapshot['lastLine'] < self.records_counter:
                self.remaining_snapshots.pop(0)

                if self.fingerprint != next_snapshot['fingerprint'] or len(self.remaining_snapshots) == 0:
                    self.__finish_validation(next_snapshot['fingerprint'])

                self.fingerprint = Snapshot.hash_fun("")

    def __finish_validation(self, fingerprint: str) -> None:
        if not self.validation_finished:
            self.validation_finished = True
            post_log_status(self.log, len(self.remaining_snapshots) == 0 and self.fingerprint == fingerprint)


class LogValidator:
    def __init__(self, agent: LfbAgent):
        self.agent = agent
        self.requests = []

        worker = threading.Thread(target=self.__worker_thread)
        worker.start()

    def __worker_thread(self) -> None:
        while True:
            new_requests = []

            for job in self.requests:
                job.do_work()

                if job.work_in_progress():
                    new_requests.append(job)

            self.requests = new_requests
            time.sleep(1)

    def __request_content(self, log: Log, snapshots: List[Dict]) -> AgentContentRequest:
        first_line, last_line = 0, 0

        if len(snapshots) > 0:
            first_line, last_line = snapshots[0]['firstLine'], snapshots[-1]['lastLine']

        return self.agent.request_log_content(log, first_line, last_line)

    def verify_log(self, log: Log) -> None:
        snapshots = get_snapshots(log.log_id)
        self.requests.append(_LogValidatorJob(snapshots, self.__request_content(log, snapshots), log))
