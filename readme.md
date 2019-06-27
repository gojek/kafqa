# KAFQA

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

Configuration of application is customisable with `kafkq.env` eg: tweak the concurrency of producers/consumers.

### Todo
* [ ] Prometheus exporter for metrics
* [ ] CI (vet/lint/golangci) (travis)
* [ ] Generate Random consumer group and topic id (for development)
* [ ] Capture throughput metrics
* [ ] measure % of data loss, average of latency
* [ ] Add more metrics on messages which're lost (ID/Sequence)


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
