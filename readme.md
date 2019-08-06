# KAFQA
[![Build Status](https://travis-ci.org/gojekfarm/kafqa.svg?branch=master)](https://travis-ci.org/gojekfarm/kafqa)

Kafka quality analyser, measuring data loss, ops, latency

### Running
* ensure go modules is enabled GO111MODULES=on if part of GOPATH and having old go version.
* ensure kafka broker mentioned in config is up.

```
source kafkqa.env && go build && ./kafkqa
```
* run `make` to run tests and linting

### Report

Tool generates report which contains the following information.

* latency: average, min, max of latency (consumption till msg received)
* Total messages sent, received and lost
* App run time

```
+---+--------------------------------+--------------+
|   |          DESCRIPTION           |    VALUE     |
+---+--------------------------------+--------------+
| 1 | Messages Lost                  |        49995 |
| 2 | Messages Sent                  |        50000 |
| 3 | Messages Received              |            5 |
| 3 | Min Consumption Latency Millis |         7446 |
| 3 | Max Consumption Latency Millis |         7461 |
| 3 | App Run Time                   | 8.801455502s |
+---+--------------------------------+--------------+
```

### Dashboard
prometheus metrics can be viewed in grafana by importing the dashboard in `scripts/dasbhoard`

### Data

Message format sent over kafka
```
message {
    sequence id
    id (unique) UUID
    timestamp
    random (size s/m/l)
}
```

### Running separate consumer and producers
* `CONSUMER_ENABLED, PRODUCER_ENABLED` can be set to only run specific component

```
# run only consumer
CONSUMER_ENABLED="true"
PRODUCER_ENABLED="false"
```
* Requires `redis` store to track and ack messages
```
STORE_TYPE="redis"
STORE_REDIS_HOST="127.0.0.1:6379"
STORE_RUN_ID="run-$CONSUMER_GROUP_ID"
```

### SSL Setup
Producer and consumer supports SSL, set the following env configuration

```
CONSUMER_SECURITY_PROTOCOL="ssl"
CONSUMER_CA_LOCATION="/certs/ca/rootCA.crt" # Public root ca certificate
CONSUMER_CERTIFICATE_LOCATION="/certs/client/client.crt" # certificate signed by ICA / root CA
CONSUMER_KEY_LOCATION="/certs/client/client.key" # private key
```

### Disable consumer Auto commit
if consumer is restarted, some messages could be not tracked, as it's committed before processing.
To disable and commit after processing the messages (This increases the run time though) set `CONSUMER_ENABLE_AUTO_COMMIT="false"

Configuration of application is customisable with `kafkq.env` eg: tweak the concurrency of producers/consumers.

### Todo
* [ ] Generate Random consumer group and topic id (for development)
* [ ] Add more metrics on messages which're lost (ID/Sequence/Duplicates)
* [ ] Producer to handle high throughput (queue full issue)
* [ ] measure % of data loss, average of latency


### Done:
* [X] convert fmt to log
* [X] Add timestamp to kafka message
* [X] Makefile
* [X] Compute lag (receive t - produce t)
* [X] Consumer
    * [X] listen to interrupt and kill consumer or stop with timeout
* [X] Add store to keep track of messages (producer) [interface]
* [X] Ack in store to for received messages (consumer)
* [X] Generate produce & consume basic report
* [X] Prometheus exporter for metrics
* [X] CI (vet/lint/golangci) (travis)
* [X] Capture throughput metrics
