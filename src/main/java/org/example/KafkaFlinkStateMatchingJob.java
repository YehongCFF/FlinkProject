package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaFlinkStateMatchingJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: <kafka-brokers> <kafka-topic> <offset-reset-strategy>");
            System.err.println("offset-reset-strategy: earliest | latest");
            return;
        }

        String kafkaBrokers = args[0];
        String kafkaTopic = args[1];
        String offsetResetStrategy = args[2];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaBrokers);
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

        OutputTag<String> matchedOutputTag = new OutputTag<String>("matched") {
        };

        SingleOutputStreamOperator<JsonNode> processed = parsedStream
                .keyBy(json -> json.get("msgId").asText())
                .process(new StateMatchingFunction(matchedOutputTag));

        processed.getSideOutput(matchedOutputTag)
                .addSink(new ConsoleSink());

        env.execute("Kafka to Flink State Matching Job");
    }

    static class ParseJsonFunction extends ProcessFunction<String, JsonNode> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
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
                // log and ignore
            }
        }
    }

    static class StateMatchingFunction extends ProcessFunction<JsonNode, JsonNode> {
        private final OutputTag<String> matchedTag;

        private transient ValueState<String> requestState;
        private transient ValueState<String> responseState;
        private transient ValueState<Boolean> pairedFlag;

        public StateMatchingFunction(OutputTag<String> matchedTag) {
            this.matchedTag = matchedTag;
        }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<String> reqDesc = new ValueStateDescriptor<>("request", String.class);
            reqDesc.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
                    .newBuilder(Time.minutes(10))
                    .cleanupInRocksdbCompactFilter(1000)
                    .build());

            ValueStateDescriptor<String> respDesc = new ValueStateDescriptor<>("response", String.class);
            respDesc.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
                    .newBuilder(Time.minutes(10))
                    .cleanupInRocksdbCompactFilter(1000)
                    .build());

            ValueStateDescriptor<Boolean> flagDesc = new ValueStateDescriptor<>("paired", Boolean.class);
            flagDesc.enableTimeToLive(org.apache.flink.api.common.state.StateTtlConfig
                    .newBuilder(Time.minutes(10))
                    .cleanupInRocksdbCompactFilter(1000)
                    .build());

            requestState = getRuntimeContext().getState(reqDesc);
            responseState = getRuntimeContext().getState(respDesc);
            pairedFlag = getRuntimeContext().getState(flagDesc);
        }

        @Override
        public void processElement(JsonNode value, Context ctx, Collector<JsonNode> out) throws Exception {
            String msgId = value.get("msgId").asText();
            int dtgrmRI = value.get("dtgrmRI").asInt();
            String content = value.get("dtgrmCntnt").asText();

            Boolean isPaired = pairedFlag.value();
            if (isPaired != null && isPaired) return; // 已配对，直接丢弃

            if (dtgrmRI == 2) { // request
                String resp = responseState.value();
                if (resp != null) {
                    ctx.output(matchedTag, "matched msgId=" + msgId);
                    responseState.clear();
                    pairedFlag.update(true);
                } else {
                    requestState.update(content); // 覆盖旧值
                }
            } else if (dtgrmRI == 3) { // response
                String req = requestState.value();
                if (req != null) {
                    ctx.output(matchedTag, "matched msgId=" + msgId);
                    requestState.clear();
                    pairedFlag.update(true);
                } else {
                    responseState.update(content);
                }
            }
        }
    }

    static class ConsoleSink extends RichSinkFunction<String> {
        private transient AtomicLong counter;

        @Override
        public void open(Configuration parameters) throws Exception {
            counter = new AtomicLong(0);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            long sequenceNumber = counter.incrementAndGet();
            System.out.println("[" + sequenceNumber + "] " + value);
        }
    }
}