# LogForgeryBlockerAgent

## [Main Project](https://github.com/LogForgeryBlocker/LogForgeryBlocker)

## Built With

- [Python](https://www.python.org/)
- [Google Protobuf](https://developers.google.com/protocol-buffers)

## Quick Start

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

- Python 3.10 or newer installed
- protoc installed

### Installation

Launch Protobuf compiler:

- `mkdir protobuf`
- `protoc -I=. --python_out=protobuf agentcom.proto`

### Usage

#### Agent

`python -m agent.agent_init`

Runs agent and starts listening for log proxies.

##### Environment

- `BACKEND_ENDPOINT` - backend server address(:port)
- `STATE_CONTROL_INTERVAL` - interval between checking current state to send snapshots to backend, and requesting config from backend
- `LOGS_CONTROL_INTERVAL`- interval for validating logs
- `AGENT_ADDR` - hostname to which bind socket for listening to new log proxy connections
- `AGENT_PORT` - port number, purpose as above
- `TOKEN` - token used to authorize to backend

<sub><sup>The .env file should be put in the working directory.</sup></sub>

#### File Proxy

`python -m proxy.filesystem.fileproxy`

Launches File Proxy, connects to specified agent and begins listening to file changes.
Currently working only on Linux platforms.

##### Environment

- `FILEPROXY_WATCHED_PATHS` - filepaths or directory names to watch. Paths should be split using the ; sign.
- `AGENT_ADDR` - agent's hostname
- `AGENT_PORT` - agent's port number

<sub><sup>The .env file should be put in the working directory.</sup></sub>