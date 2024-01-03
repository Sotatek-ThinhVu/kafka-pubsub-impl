package com.example.kafkapubsub.producer;

import com.example.kafkapubsub.model.User;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


@Service
@RequiredArgsConstructor
    public class ConsumerService extends AbstractConsumerSeekAware {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    private final Map<Thread, ConsumerSeekCallback> callbackForThread = new ConcurrentHashMap<>();

    private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

    private final Map<ConsumerSeekCallback, List<TopicPartition>> callbacksToTopic = new ConcurrentHashMap<>();

    private void waitUntil(long t) {
        try {
            Thread.sleep(t);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        System.out.println("Assignments:" + assignments);
        super.onPartitionsAssigned(assignments, callback);
        assignments.keySet().forEach(partition -> {
            callback.seek(partition.topic(),partition.partition(),15);
        });
//        callback.seek("userTopic", 0,12);
    }

    @KafkaListener(topics = "userTopic",
            groupId = "group1",
            containerFactory = "userKafkaListenerFactory")
    public void listener1(ConsumerRecord<String, User> record,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET)int offset,
                          Acknowledgment acknowledgment) {
        //callbackForThread.get(Thread.currentThread()).seek("uesrTopic",0,20);
        User user = record.value();
        logger.info("Listener 1-Group1: "+user.getFullname()+" from partition: " + partition + " with offset: "+offset);
        //throwException();
        acknowledgment.acknowledge();
    }


    @KafkaListener(topics = "adminTopic",
            groupId = "group1",
            containerFactory = "userKafkaListenerFactory")
    public void listener5(ConsumerRecord<String, User> record,
                          @Header(KafkaHeaders.RECEIVED_PARTITION)int partition,
                          @Header(KafkaHeaders.OFFSET)int offset,
                          Acknowledgment acknowledgment) {
        User user = record.value();
        logger.info("Listener 5-Group2: "+user.getFullname()+" from partition: " + partition + " with offset: "+offset);
        //throwException();
        acknowledgment.acknowledge();
    }

    private void throwException(){
        throw new RuntimeException("Error");
    }

//    @KafkaListener(topics = "adminTopic",
//            groupId = "group2",
//            containerFactory = "userKafkaListenerFactory")
//    public void listener2(ConsumerRecord<String, User> record,
//                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
//        User user = record.value();
//        logger.info("Listener 2-Group1: "+user.getFullname()+ " from partition: " + partition);
//    }

//    @KafkaListener(
//            topicPartitions = @TopicPartition(topic = "userTopic",
//                    partitionOffsets = {
//                            @PartitionOffset(partition = "0", initialOffset = "0")}),
//            containerFactory = "userKafkaListenerFactory")
//    public void listener3(
//            @Payload User user,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
//        System.out.println(
//                "Listener 3-Group1: Received: " + user.getFullname()
//                        + " from partition: " + partition);
//    }

//    @KafkaListener(
//            topicPartitions = @TopicPartition(topic = "userTopic",
//                    partitionOffsets = {
//                            @PartitionOffset(partition = "0", initialOffset = "10")}),
//            containerFactory = "userKafkaListenerFactory")
//    public void listener4(
//            @Payload User user,
//            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
//        System.out.println(
//                "Listener 4-Group1: Received: " + user.getFullname()
//                        + " from partition: " + partition);
//    }
}
