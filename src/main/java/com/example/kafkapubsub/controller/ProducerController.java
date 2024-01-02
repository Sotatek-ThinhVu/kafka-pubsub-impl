package com.example.kafkapubsub.controller;

import com.example.kafkapubsub.model.Message;
import com.example.kafkapubsub.model.MessageDto;
import com.example.kafkapubsub.model.User;
import com.example.kafkapubsub.producer.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/producer")
public class ProducerController {

    @Autowired
    private ProducerService producerService;

    @PostMapping("/userTopic")
    public ResponseEntity<?> producerUserToKafka(@RequestBody User user){
        producerService.producerUser(user);
        return ResponseEntity.ok(new MessageDto("Okay"));
    }

    @PostMapping("/adminTopic")
    public ResponseEntity<?> producerUserToKafka1(@RequestBody User user){
        producerService.producerUser1(user);
        return ResponseEntity.ok(new MessageDto("Okay"));
    }

//    @PostMapping("/userTopic/massage")
//    public ResponseEntity<?> producerMessageToKafka2(@RequestBody Message message){
//        producerService.producerMessage(message);
//        return ResponseEntity.ok(new MessageDto("Okay"));
//    }
}
