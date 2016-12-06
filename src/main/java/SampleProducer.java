import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by kunch on 11/21/16.
 */
public class SampleProducer {
    private static final Properties props = new Properties();
    private final static Logger LOG = LoggerFactory.getLogger(SampleProducer.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Sample Producer initialized.");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 2; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i) ,"messages from java plugin."));
        }
        producer.close();
    }
}
