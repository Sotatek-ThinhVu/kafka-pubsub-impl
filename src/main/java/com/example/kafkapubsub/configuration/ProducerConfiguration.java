package com.example.kafkapubsub.configuration;

import com.example.kafkapubsub.model.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfiguration {

    @Value("${kafka.brokerAddress}")
    private String bootstrap_servers;

    @Bean
    public <K, V> ProducerFactory<K, V> userProducerFactory(){
        Map<String,Object> props = createDefaultProps();

        //props.put(JsonSerializer.TYPE_MAPPINGS,"User:com.example.kafkapubsub.model.User,Message:com.example.kafkapubsub.model.Message");
        return new DefaultKafkaProducerFactory(props);
    }

    @Bean
    public KafkaTemplate<String, User> userKafkaTemplate(){
        return new KafkaTemplate<>(userProducerFactory());
    }

    private Map<String, Object> createDefaultProps() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return props;
    }
}
