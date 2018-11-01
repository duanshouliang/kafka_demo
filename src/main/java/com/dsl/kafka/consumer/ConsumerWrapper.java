package com.dsl.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

/**
 * Created by slsir on 2018/11/1.
 */
public class ConsumerWrapper {

    private List<String> topics;
    private String groupId;
    private String bootstrapServers;
    private Properties configs = new Properties();

    public ConsumerWrapper(List<String> topics, String groupId, String bootstrapServers) {
        this.topics = topics;
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        init();
    }

    private void init() {
        Properties props = new Properties();
        /* 定义kakfa 服务的地址，不需要将所有broker指定上 */
        configs.put("bootstrap.servers", bootstrapServers);
        /* 制定consumer group */
        configs.put("group.id", groupId);
        /* 是否自动确认offset */
        configs.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        configs.put("auto.commit.interval.ms", "1000");
        configs.put("session.timeout.ms", "30000");
        /* key的序列化类 */
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public void consumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                System.out.println();
            }
        }
    }
}
