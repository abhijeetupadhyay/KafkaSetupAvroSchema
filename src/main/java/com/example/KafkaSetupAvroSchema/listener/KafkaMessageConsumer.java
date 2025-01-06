package com.example.KafkaSetupAvroSchema.listener;

import com.example.KafkaSetupAvroSchema.model.Student;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageConsumer {
    @KafkaListener(topics = "shine-test-local-avro-topic", groupId = "shine-local-avro")
    public void listen(Student message) {
        System.out.println("Received Messasge in group: " + message);
        System.out.println("Received Student Name: " + message.getStudentName());
        System.out.println("Received Student Id: " + message.getStudentId());
        System.out.println("Received Student Age: " + message.getAge());
    }
}
