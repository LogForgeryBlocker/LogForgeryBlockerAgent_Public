import argparse, os, threading

from agent.lfb_agent import LfbAgent
from agent.collector import ListLogCollector
from agent.sched import LFBSched
from agent.validator import LogValidator
from agent.server import AgentServer
from dotenv import load_dotenv

load_dotenv('.env')


parser = argparse.ArgumentParser()
parser.add_argument('--addr', '-a', type=str)
parser.add_argument('--port', '-p', type=int)
args = parser.parse_args()

address = args.addr or os.environ['AGENT_ADDR']
port = args.port or int(os.environ['AGENT_PORT'])

print(f'Server listening on {address}:{port}')

log_collector = ListLogCollector()
server = AgentServer(log_collector, address, port)

agent = LfbAgent()
validator = LogValidator(agent)

sched = LFBSched(log_collector, validator)
sched.start()

process_thread = threading.Thread(target=agent.handle_clients)
process_thread.start()

while True:
    con = server.accept_new()
    print(f'Accepting new connection from {con.get_address()}:{con.get_port()}')
    agent.add_client(con)
