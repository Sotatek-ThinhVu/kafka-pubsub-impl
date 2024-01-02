package com.example.kafkapubsub.producer;

import com.example.kafkapubsub.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;


@RequiredArgsConstructor
@Service
public class ProducerService {

    private final KafkaTemplate<String, User> kafkaTemplate;

    @Value("${kafka.user.topicName}")
    private String topicName ;

    public void producerUser(User user){
//        ProducerRecord<String,User> producerRecord = new ProducerRecord<>(topicName,user);
//        System.out.println("Receive: " +user);
//        kafkaTemplate.send(producerRecord);
        CompletableFuture<SendResult<String, User>> future = kafkaTemplate.send(topicName, user);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + user.getFullname() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        user.getFullname() + "] due to : " + ex.getMessage());
            }
        });
    }

    public void producerUser1(User user){
        CompletableFuture<SendResult<String, User>> future = kafkaTemplate.send("adminTopic", user);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + user.getFullname() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        user.getFullname() + "] due to : " + ex.getMessage());
            }
        });
    }

//    public void producerMessage(Message message){
//        CompletableFuture<SendResult<String, User>> future = kafkaTemplate.send(topicName, message);
//        future.whenComplete((result, ex) -> {
//            if (ex == null) {
//                System.out.println("Sent message=[" + message.getMessage() +
//                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
//            } else {
//                System.out.println("Unable to send message=[" +
//                        message.getMessage() + "] due to : " + ex.getMessage());
//            }
//        });
//    }
}
