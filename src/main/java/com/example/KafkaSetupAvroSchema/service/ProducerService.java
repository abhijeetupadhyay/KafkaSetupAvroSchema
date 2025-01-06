package com.example.KafkaSetupAvroSchema.service;

import com.example.KafkaSetupAvroSchema.model.Student;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class ProducerService {
    @Value("${kafka.topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Student> kafkaTemplate;

    public void sendMessage(Student message) {
        CompletableFuture<SendResult<String, Student>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            } else {
                // log.error("Unable to send message=[{}] due to : {}", message, ex.getMessage());
            }
        });
    }
}
