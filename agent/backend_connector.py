import json
import os
import sys
from dotenv import load_dotenv
import requests

from agent.log import Snapshot, Log

load_dotenv('.env')


def __handle_response(res):
    if res['success']:
        return res['data']
    else:
        print("Server Response Error\n" + json.dumps(res), file=sys.stderr)
        os._exit(1)


def __post_json(data_dict, endpoint):
    res = requests.post(os.environ.get('BACKEND_ENDPOINT') + endpoint, json=data_dict,
                        headers={"Authorization": f"Bearer {os.environ.get('TOKEN')}"}).json()

    return __handle_response(res)


def __get_json(endpoint):
    res = requests.get(os.environ.get('BACKEND_ENDPOINT') + endpoint,
                       headers={"Authorization": f"Bearer {os.environ.get('TOKEN')}"}).json()

    return __handle_response(res)


def __parse_snapshots(snapshots_raw):
    ret = {}

    for snapshot in snapshots_raw:
        ret[snapshot['name']] = Snapshot(Log(snapshot['name'], snapshot['id']), snapshot['records'])

    return ret


def post_log(log_name):
    ret = Log(log_name)
    ret.add_log_id(__post_json(ret.to_dict(), 'log')['id'])

    return Snapshot(ret)


def post_snapshot(snapshot):
    data = snapshot.upload_prep()

    if data['firstLine'] <= data['lastLine']:
        return __post_json(data, 'snapshot')


def post_log_status(log, status):
    __post_json({"isCorrect": status}, 'log/' + str(log.log_id) + "/verification")


def get_logs():
    return __parse_snapshots(__get_json('log/for_agent'))


def get_snapshots(log_id):
    return __get_json('snapshot/agent_for_log/' + log_id)


def get_config():
    return __get_json('agent/config')
