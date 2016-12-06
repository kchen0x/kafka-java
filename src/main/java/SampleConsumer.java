import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kunch on 11/21/16.
 *
 */
public class SampleConsumer {
    private static final Properties props = new Properties();

    public static void main(String[] args) throws Exception {
        // config
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // create consumer with config
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // subscribe 'topic' from Kafka
        consumer.subscribe(Arrays.asList("test"));
        while (true) {
            //get data
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
        }
    }
}