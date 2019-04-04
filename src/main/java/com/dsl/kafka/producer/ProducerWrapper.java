package com.dsl.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * Created by duanshuliang on 2018/11/1.
 */
public class ProducerWrapper {
    private String topic;
    private String acks;
    private Properties configs = new Properties();
    private KafkaProducer<String, String> producer;

    public ProducerWrapper(String topic, String acks) {
        this.topic = topic;
        this.acks = acks;
        init();
        producer = new KafkaProducer<String, String>(configs);
    }

    void init() {
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("acks", acks);
        configs.put("retries", 0);
        configs.put("batch.size", 16384);
        configs.put("linger.ms", 1);
        configs.put("buffer.memory", 33554432);
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void send(String data) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, null, data);
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println(exception.getMessage());
                }
            }
        });
    }
}
