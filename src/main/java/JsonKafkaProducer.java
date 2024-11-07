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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JsonKafkaProducer {
    private static Logger logger = LogManager.getRootLogger();

    private static String TOPIC_NAME;
    private static String BOOTSTRAP_SERVERS;  
    private static int rate_mbps=0;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static long recordCounter = 0;

    public static void main(String[] args) {
        // Set up Kafka producer configuration
        Properties props = new Properties();
	BOOTSTRAP_SERVERS = args[0];
	TOPIC_NAME = args[1];
        rate_mbps = Integer.parseInt(args[2]);

        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Create a Kafka producer
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
		long start_time = System.currentTimeMillis();
		long records_ps = sleep_calculator(rate_mbps);
		double buffer = 1;
		for ( long i = 0; i < records_ps * buffer; i ++) {

                	// Generate JSON record and send it to Kafka
                	String jsonRecord = createJsonRecord();
                	ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, jsonRecord);

                	// Send record asynchronously
                	producer.send(record, (RecordMetadata metadata, Exception e) -> 
			{
                    		if (e != null) {
                        		logger.error (e);
                    		} else {
                        		logger.trace("Record sent with offset " + metadata.offset() + " to partition , " + metadata.partition());
                    		}
			});
		}

	        long stop_time = System.currentTimeMillis();
                if ( stop_time - start_time < 1000 ) {
                        buffer =1;
                        Thread.sleep( 1000 - stop_time - start_time);
                }
                if (stop_time - start_time > 1000 ) {
                         double new_buffer = 1 + (( stop_time - start_time - 1000 ) / 1000 );
			 logger.warn("Slow to write " + records_ps * buffer + " in 1000ms increasing buffer by " + (1+new_buffer));
			 buffer = new_buffer;
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Method to generate JSON record
    protected static String createJsonRecord() {
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

    private static long sleep_calculator(int rate_mbps) {
	    int record_size = createJsonRecord().length();
            long bps = rate_mbps * 1024*1024;
	    long record_count_ps = bps / record_size;
	    return record_count_ps;
    }
}

