package com.example.kafkapubsub.producer;

import com.example.kafkapubsub.model.User;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ConsumerService {

    @KafkaListener(topics = "userTopic",
            groupId = "group1",
            containerFactory = "userKafkaListenerFactory")
    public void listener1(ConsumerRecord<String, User> record,
                            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        User user = record.value();
        System.out.println("Listener 1-Group1: Consume user with fullname "+user.getFullname()+" from partition: " + partition);
    }

    @KafkaListener(topics = "adminTopic",
            groupId = "group1",
            containerFactory = "userKafkaListenerFactory")
    public void listener5(ConsumerRecord<String, User> record,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        User user = record.value();
        System.out.println("Listener 5-Group2: Received "+user.getFullname()+" from partition: " + partition);
    }

    @KafkaListener(topics = "adminTopic",
            groupId = "group2",
            containerFactory = "userKafkaListenerFactory")

    public void listener2(ConsumerRecord<String, User> record,
                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        User user = record.value();
        System.out.println("Listener 2-Group1: Received: "+user.getFullname()+ " from partition: " + partition);
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "userTopic",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "0")}),
            containerFactory = "userKafkaListenerFactory")
    public void listener3(
            @Payload User user,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println(
                "Listener 3-Group1: Received: " + user.getFullname()
                        + " from partition: " + partition);
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "userTopic",
                    partitionOffsets = {
                            @PartitionOffset(partition = "0", initialOffset = "10")}),
            containerFactory = "userKafkaListenerFactory")
    public void listener4(
            @Payload User user,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println(
                "Listener 4-Group1: Received: " + user.getFullname()
                        + " from partition: " + partition);
    }
}
