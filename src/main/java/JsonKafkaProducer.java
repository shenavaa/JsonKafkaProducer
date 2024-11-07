import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class JsonKafkaProducer {

    private static String TOPIC_NAME;
    private static String BOOTSTRAP_SERVERS;  
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static long recordCounter = 0;

    public static void main(String[] args) {
        // Set up Kafka producer configuration
        Properties props = new Properties();
	BOOTSTRAP_SERVERS = args[0];
	TOPIC_NAME = args[1];

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Create a Kafka producer
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                // Generate JSON record and send it to Kafka
                String jsonRecord = createJsonRecord();
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, jsonRecord);

                // Send record asynchronously
                producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Record sent with offset %d to partition %d%n", metadata.offset(), metadata.partition());
                    }
                });

                // Sleep for a short period if necessary to control the sending rate
                Thread.sleep(200); // Adjust as per your rate requirement
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to generate JSON record
    private static String createJsonRecord() {
        Map<String, Object> record = new HashMap<>();
        record.put("samplecode", Instant.now().getEpochSecond());
        record.put("uuid", UUID.randomUUID().toString());
        record.put("recordCount", ++recordCounter);

        try {
            return objectMapper.writeValueAsString(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize JSON record", e);
        }
    }
}

