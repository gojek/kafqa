# KAFQA

Kafka quality analyser, measuring data loss, ops, latency

### TODO
* [ ] Compute lag (receive t - produce t)
* [ ] Consumer
    * [ ] listen to interrupt and kill consumer or stop with timeout
* [ ] Add store to keep track of messages (producer) [interface]
* [ ] Ack in store to for received messages (consumer)
* [ ] Generate produce & consume report
* [ ] Prometheus exporter for metrics
* [ ] CI (vet/lint/golangci) (travis)

### Report

Need to generate report which have the following information

latency:
 * 99, 95, percentiles of latency (msg received)
 * average, min, max of latency (msg received)

failures:
 * publish to kafka
 * data loss

* change in brokers (dns, addition/deletion of brokers) / metadata, consumer rebalance
* total messages success and failure %
* TPS ( m/5m/10m )

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

### Done:
* [X] convert fmt to log
* [X] Add timestamp to kafka message
* [X] Makefile
