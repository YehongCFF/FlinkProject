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
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

        // 启用对象重用以减少GC压力
        env.getConfig().enableObjectReuse();

        // 设置合理的检查点间隔
        env.enableCheckpointing(30000); // 30秒检查点间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);

        // 使用时间戳作为consumer group ID的一部分，确保每次都是新的consumer group
        String consumerGroupId = "flink-group-" + System.currentTimeMillis();
        if ("earliest".equals(offsetResetStrategy)) {
            // 如果要从最早开始消费，使用唯一的consumer group
            kafkaProps.setProperty("group.id", consumerGroupId);
        } else {
            // 如果要从最新开始消费，可以使用固定的consumer group
            kafkaProps.setProperty("group.id", "flink-group");
        }

        kafkaProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        kafkaProps.setProperty("sasl.kerberos.service.name", "kafka");
        kafkaProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("auto.offset.reset", offsetResetStrategy);

        // 优化Kafka消费者配置
        kafkaProps.setProperty("fetch.min.bytes", "1048576"); // 1MB
        kafkaProps.setProperty("fetch.max.wait.ms", "500");
        kafkaProps.setProperty("max.partition.fetch.bytes", "2097152"); // 2MB
        kafkaProps.setProperty("session.timeout.ms", "30000");
        kafkaProps.setProperty("heartbeat.interval.ms", "3000");

        // 关闭自动提交offset，让Flink管理offset
        kafkaProps.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                kafkaTopic,
                new SimpleStringSchema(),
                kafkaProps
        );

        // 根据offsetResetStrategy设置起始位置
        if ("earliest".equals(offsetResetStrategy)) {
            kafkaConsumer.setStartFromEarliest();
        } else if ("latest".equals(offsetResetStrategy)) {
            kafkaConsumer.setStartFromLatest();
        }

        // 如果要强制从earliest开始，可以使用以下方式：
        // kafkaConsumer.setStartFromGroupOffsets(); // 默认行为，从group offsets开始
        // kafkaConsumer.setStartFromEarliest(); // 强制从最早开始
        // kafkaConsumer.setStartFromLatest(); // 强制从最新开始
        // kafkaConsumer.setStartFromTimestamp(timestamp); // 从指定时间戳开始

        DataStream<String> stream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        DataStream<JsonNode> parsedStream = stream.process(new ParseJsonFunction());

        parsedStream.keyBy(json -> json.get("msgId").asText())
                .addSink(new RedisPairSink(redisNodes));

        System.out.println("Starting Kafka to Redis Pairing Job with offset reset strategy: " + offsetResetStrategy);
        System.out.println("Consumer group ID: " + kafkaProps.getProperty("group.id"));

        env.execute("Kafka to Redis Pairing Job");
    }

    static class ParseJsonFunction extends ProcessFunction<String, JsonNode> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapper = new ObjectMapper();
            // 优化Jackson配置
            mapper.getFactory().disable(com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            mapper.getFactory().disable(com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE);
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
                System.err.println("Failed to parse JSON: " + value + ", error: " + e.getMessage());
            }
        }
    }

    static class RedisPairSink extends RichSinkFunction<JsonNode> {
        private final String redisNodesConfig;
        private transient JedisCluster jedisCluster;
        private transient long lastBatchTime;
        private static final int BATCH_SIZE = 100;
        private static final long BATCH_TIMEOUT_MS = 1000;

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

            // 使用默认连接池配置创建JedisCluster
            jedisCluster = new JedisCluster(nodes, "default", "123456");
            lastBatchTime = System.currentTimeMillis();
        }

        @Override
        public void invoke(JsonNode value, Context context) throws Exception {
            String msgId = value.get("msgId").asText();
            int dtgrmRI = value.get("dtgrmRI").asInt();
            String cntnt = value.get("dtgrmCntnt").asText();

            String requestKey = "req:" + msgId;
            String responseKey = "resp:" + msgId;
            String pairKey = "paired:" + msgId;

            // 使用Redis管道优化批量操作
            try {
                // 先检查是否已经配对
                if (jedisCluster.exists(pairKey)) {
                    return;
                }

                if (dtgrmRI == 2) {
                    // 使用MULTI/EXEC事务确保原子性
                    String luaScript =
                            "local responseKey = KEYS[1] " +
                                    "local requestKey = KEYS[2] " +
                                    "local pairKey = KEYS[3] " +
                                    "local content = ARGV[1] " +
                                    "if redis.call('EXISTS', responseKey) == 1 then " +
                                    "  redis.call('DEL', responseKey) " +
                                    "  redis.call('SETEX', pairKey, 600, '1') " +
                                    "  return 'matched' " +
                                    "else " +
                                    "  redis.call('SETEX', requestKey, 600, content) " +
                                    "  return 'stored' " +
                                    "end";

                    jedisCluster.eval(luaScript, 3, responseKey, requestKey, pairKey, cntnt);

                } else if (dtgrmRI == 3) {
                    String luaScript =
                            "local requestKey = KEYS[1] " +
                                    "local responseKey = KEYS[2] " +
                                    "local pairKey = KEYS[3] " +
                                    "local content = ARGV[1] " +
                                    "if redis.call('EXISTS', requestKey) == 1 then " +
                                    "  redis.call('DEL', requestKey) " +
                                    "  redis.call('SETEX', pairKey, 600, '1') " +
                                    "  return 'matched' " +
                                    "else " +
                                    "  redis.call('SETEX', responseKey, 600, content) " +
                                    "  return 'stored' " +
                                    "end";

                    jedisCluster.eval(luaScript, 3, requestKey, responseKey, pairKey, cntnt);
                }
            } catch (Exception e) {
                // 如果Lua脚本失败，回退到原来的逻辑
                fallbackToOriginalLogic(msgId, dtgrmRI, cntnt, requestKey, responseKey, pairKey);
            }
        }

        private void fallbackToOriginalLogic(String msgId, int dtgrmRI, String cntnt,
                                             String requestKey, String responseKey, String pairKey) throws Exception {
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
            if (jedisCluster != null) {
                jedisCluster.close();
            }
        }
    }
}