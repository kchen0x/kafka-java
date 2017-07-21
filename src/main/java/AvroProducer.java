import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * This is for avro producer test.
 * Created by kunch on 12/19/16.
 */
public class AvroProducer {
    public static void main(String[] args) throws Exception {
        //TODO
        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.25.135:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(props);

        String SCHEMA = "{\n" +
                "  \"fields\": [\n" +
                "    { \"name\": \"timestamp\", \"type\": \"string\" },\n" +
                "    { \"name\": \"sensor_group\", \"type\": \"string\" },\n" +
                "    { \"name\": \"sensor\", \"type\": \"string\" },\n" +
                "    { \"name\": \"uuid\", \"type\": \"string\"},\n" +
                "    { \"name\": \"value\", \"type\": \"string\" }\n" +
                "  ],\n" +
                "  \"name\": \"sensorRecord\",\n" +
                "  \"type\": \"record\"\n" +
                "}";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(SCHEMA);
        GenericRecord avroRecord = new GenericData.Record(schema);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        avroRecord.put("timestamp", dateFormat.format(new Date()));
        avroRecord.put("sensor_group", "sg_1");
        avroRecord.put("sensor", "s_1");
        avroRecord.put("uuid", "uuid_007");
        avroRecord.put("value", "test data");


        DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(avroRecord, encoder);
        encoder.flush();
        out.close();
        byte[] bytesRecord  = out.toByteArray();

        ProducerRecord<String, byte[]> record =
                new ProducerRecord<String, byte[]>("clb.demo.depa.raw", bytesRecord);
        producer.send(record);
    }
}
