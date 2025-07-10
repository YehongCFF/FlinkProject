package org.example;

import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonDataGenerator {

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random RANDOM = new Random();
    private static final Gson GSON = new GsonBuilder().create();
    private static final int THREAD_COUNT = 8; // 可根据性能调整并发线程数

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.out.println("Usage: java JsonDataGenerator <total_count> <pair_ratio> <kafka_bootstrap_servers> <kafka_topic>");
            return;
        }

        int totalCount = Integer.parseInt(args[0]);
        double pairRatio = Double.parseDouble(args[1]);
        String kafkaBrokers = args[2];
        String kafkaTopic = args[3];

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        // 性能优化参数
        props.put("batch.size", 32768);
        props.put("linger.ms", 20);
        props.put("acks", "1");
        props.put("compression.type", "lz4");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int pairCount = (int) ((1.0 - pairRatio) * totalCount / 2);
        int unpairedCount = totalCount - (pairCount * 2);

        List<String> messageBuffer = new ArrayList<>();

        for (int i = 0; i < pairCount; i++) {
            String msgId = generateRandomString(32);
            messageBuffer.add(GSON.toJson(createData(msgId, 2, "request" + generateRandomString(32))));
            messageBuffer.add(GSON.toJson(createData(msgId, 3, "response" + generateRandomString(32))));
        }

        for (int i = 0; i < unpairedCount; i++) {
            String msgId = generateRandomString(32);
            int type = RANDOM.nextBoolean() ? 2 : 3;
            String prefix = type == 2 ? "request" : "response";
            messageBuffer.add(GSON.toJson(createData(msgId, type, prefix + generateRandomString(32))));
        }

        Collections.shuffle(messageBuffer);

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(messageBuffer.size());

        for (String message : messageBuffer) {
            executor.submit(() -> {
                try {
                    producer.send(new ProducerRecord<>(kafkaTopic, null, message), (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to send: " + exception.getMessage());
                        }
                        latch.countDown();
                    });
                } catch (Exception e) {
                    System.err.println("Send error: " + e.getMessage());
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        producer.close();

        System.out.println("All messages sent to Kafka topic: " + kafkaTopic);
    }

    private static Map<String, String> createData(String msgId, int dtgrmRI, String dtgrmCntnt) {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("msgId", msgId);
        map.put("dtgrmRI", String.valueOf(dtgrmRI));
        map.put("dtgrmCntnt", dtgrmCntnt);
        return map;
    }

    private static String generateRandomString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(RANDOM.nextInt(CHARACTERS.length())));
        }
        return sb.toString();
    }
}
