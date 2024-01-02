package com.example.kafkapubsub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaPubsubApplication {
	private static final String TEST_TOPIC = "test-topic";
	private static final String TEST_GROUP = "test-group";

	private static final String TEST_GROUP_NEW = "test-group-new";

	public static void main(String[] args) {
		SpringApplication.run(KafkaPubsubApplication.class, args);
	}

//	@KafkaListener(topics = TEST_TOPIC, groupId = TEST_GROUP)
//	public void listener1(String message) {
//		System.out.println("Message: '" + message + "' from listener1");
//	}
//
//	@KafkaListener(topics = TEST_TOPIC, groupId = TEST_GROUP)
//	public void listener2(String message) {
//		System.out.println("Message: '" + message + "' from listener2");
//	}
//
//	@KafkaListener(topics = TEST_TOPIC, groupId = TEST_GROUP_NEW)
//	public void listener3(String message) {
//		System.out.println("Message: '" + message + "' from listener3");
//	}
}
