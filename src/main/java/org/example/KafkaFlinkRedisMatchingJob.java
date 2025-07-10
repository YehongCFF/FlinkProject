package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class KafkaFlinkRedisMatchingJob {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: KafkaFlinkRedisMatchingJob <kafka-bootstrap-servers> <kafka-topic> <redis-nodes> <offset-reset-strategy>");
            System.err.println("offset-reset-strategy: earliest | latest");
            return;
        }

        String kafkaBootstrapServers = args[0]; // e.g. "spdb01:9092,spdb02:9092,spdb03:9092"
        String kafkaTopic = args[1]; // e.g. "test-lyh02"
        String redisNodes = args[2]; // e.g. "spdb01:6399,spdb02:6399,spdb03:6399"
        String offsetResetStrategy = args[3]; // "earliest" or "latest"

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
        kafkaProps.setProperty("group.id", "flink-group");
        kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.setProperty("sasl.kerberos.service.name", "kafka");
        kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("auto.offset.reset", offsetResetStrategy);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                kafkaTopic,
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<String> stream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        DataStream<JsonNode> parsedStream = stream.process(new ParseJsonFunction());

        parsedStream.keyBy(json -> json.get("msgId").asText())
                .addSink(new RedisPairSink(redisNodes));

        env.execute("Kafka to Redis Pairing Job");
    }

    static class ParseJsonFunction extends ProcessFunction<String, JsonNode> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<JsonNode> out) throws Exception {
            try {
                JsonNode json = mapper.readTree(value);
                if (json.has("msgId") && json.has("dtgrmRI") && json.has("dtgrmCntnt")) {
                    out.collect(json);
                }
            } catch (Exception e) {
                // log and skip invalid json
            }
        }
    }

    static class RedisPairSink extends RichSinkFunction<JsonNode> {
        private final String redisNodesConfig;
        private transient JedisCluster jedisCluster;

        public RedisPairSink(String redisNodesConfig) {
            this.redisNodesConfig = redisNodesConfig;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Set<HostAndPort> nodes = new HashSet<>();
            for (String node : redisNodesConfig.split(",")) {
                String[] parts = node.split(":");
                nodes.add(new HostAndPort(parts[0], Integer.parseInt(parts[1])));
            }
            jedisCluster = new JedisCluster(nodes, "default", "123456");
        }

        @Override
        public void invoke(JsonNode value, Context context) throws Exception {
            String msgId = value.get("msgId").asText();
            int dtgrmRI = value.get("dtgrmRI").asInt();
            String cntnt = value.get("dtgrmCntnt").asText();

            String requestKey = "req:" + msgId;
            String responseKey = "resp:" + msgId;
            String pairKey = "paired:" + msgId;

            if (jedisCluster.exists(pairKey)) {
                return;
            }

            if (dtgrmRI == 2) {
                if (jedisCluster.exists(responseKey)) {
                    jedisCluster.del(responseKey);
                    jedisCluster.setex(pairKey, 600, "1");
                } else {
                    jedisCluster.setex(requestKey, 600, cntnt);
                }
            } else if (dtgrmRI == 3) {
                if (jedisCluster.exists(requestKey)) {
                    jedisCluster.del(requestKey);
                    jedisCluster.setex(pairKey, 600, "1");
                } else {
                    jedisCluster.setex(responseKey, 600, cntnt);
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (jedisCluster != null) jedisCluster.close();
        }
    }
}