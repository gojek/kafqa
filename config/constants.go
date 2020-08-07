package config

const KafkaBootstrapServerKey string = "bootstrap.servers"
const ConsumerGroupIDKey string = "group.id"
const ConsumerOffsetResetKey string = "auto.offset.reset"
const EnableAutoCommit string = "enable.auto.commit"

const SecurityProtocol string = "security.protocol"
const SSLCertificateLocation string = "ssl.certificate.location"
const SSLCALocation string = "ssl.ca.location"
const SSLKeyLocation string = "ssl.key.location"
const SSLCertLocation string = "ssl.certificate.location"
const SSLKeyPassword string = "ssl.key.password"

const ProducerQueueBufferingMaxMessages string = "queue.buffering.max.messages"
const ProducerBatchNumMessages string = "batch.num.messages"
const ProduceRequestRequiredAcks string = "request.required.acks"

const ConsumerQueuedMinMessages string = "queued.min.messages"

const LibrdStatisticsIntervalMs string = "statistics.interval.ms"
const CompressionType string = "compression.type"
