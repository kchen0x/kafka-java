import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by kunch on 11/21/16.
 */
public class SampleProducerAndConsumer {
    private static final Properties prodProps = new Properties();
    private static final Properties consProps = new Properties();

    public static void main(String[] args) throws Exception {
        prodProps.put("bootstrap.servers", "localhost:9092");
        prodProps.put("acks", "all");
        prodProps.put("retries", 0);
        prodProps.put("batch.size", 16384);
        prodProps.put("linger.ms", 1);
        prodProps.put("buffer.memory", 33554432);
        prodProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prodProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        consProps.put("bootstrap.servers", "localhost:9092");
        consProps.put("group.id", "test");
        consProps.put("enable.auto.commit", "true");
        consProps.put("auto.commit.interval.ms", "1000");
        consProps.put("session.timeout.ms", "5000");
        consProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(prodProps);
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(consProps);

        InnerProducer innerProducer = new InnerProducer(producer);
        InnerConsumer innerConsumer = new InnerConsumer(consumer);

        innerConsumer.start();
        innerProducer.start();
    }

    private static class InnerProducer extends Thread {
        private Producer<String, String> producer;
        private int count;

        InnerProducer(Producer<String, String> producer) {
            this.producer = producer;
            count = 0;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    producer.send(new ProducerRecord<String, String>("test", "Java index", Integer.toString(count)));
                    count ++;
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class InnerConsumer extends Thread {
        private  Consumer<String, String> consumer;

        InnerConsumer(Consumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            consumer.subscribe(Collections.singletonList("test"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());
            }
        }
    }
}

