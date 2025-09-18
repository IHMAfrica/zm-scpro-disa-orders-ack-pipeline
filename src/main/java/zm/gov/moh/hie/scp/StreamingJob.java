package zm.gov.moh.hie.scp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zm.gov.moh.hie.scp.deserializer.OrderAckDeserializer;
import zm.gov.moh.hie.scp.dto.OrderAck;
import zm.gov.moh.hie.scp.sink.OrderAckJdbcSink;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        // Load effective configuration (CLI > env > defaults)
        final Config cfg = Config.fromEnvAndArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build Kafka source for ACK messages
        KafkaSource<OrderAck> source = KafkaSource.<OrderAck>builder()
                .setBootstrapServers(cfg.kafkaBootstrapServers)
                .setTopics(cfg.kafkaTopic)
                .setGroupId(cfg.kafkaGroupId)
                .setProperty("enable.auto.commit", "true")
                .setProperty("auto.commit.interval.ms", "2000")
                .setProperty("max.poll.interval.ms", "10000")
                .setProperty("max.poll.records", "50")
                .setProperty("request.timeout.ms", "2540000")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("default.api.timeout.ms", "2540000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new OrderAckDeserializer())
                .setProperty("security.protocol", cfg.kafkaSecurityProtocol)
                .setProperty("sasl.mechanism", cfg.kafkaSaslMechanism)
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                                "username=\"" + cfg.kafkaSaslUsername + "\" " +
                                "password=\"" + cfg.kafkaSaslPassword + "\";")
                .build();

        // Create a DataStream from Kafka source
        DataStream<OrderAck> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka ACK Source"
        ).startNewChain();

        // Filter out null values to prevent issues with the sink
        DataStream<OrderAck> filteredStream = kafkaStream
                .filter(orderAck -> {
                    if (orderAck == null) {
                        LOG.warn("Filtered out null OrderAck");
                        return false;
                    }
                    if (orderAck.getOrderMessageRefId() != null) {
                        LOG.info("Processing OrderAck with message control ID: {}", orderAck.getOrderMessageRefId());
                    } else {
                        LOG.info("Processing OrderAck with no message control ID");
                    }
                    return true;
                })
                .name("Filter Null Values").disableChaining();

        // Create JDBC sink for order acknowledgments
        final var jdbcSink = OrderAckJdbcSink.getSinkFunction(cfg.jdbcUrl, cfg.jdbcUser, cfg.jdbcPassword);

        filteredStream.addSink(jdbcSink).name("Postgres JDBC -> Order ACK Sink");

        // Execute the pipeline
        env.execute("Kafka to Postgres Orders ACK Pipeline");
    }
}