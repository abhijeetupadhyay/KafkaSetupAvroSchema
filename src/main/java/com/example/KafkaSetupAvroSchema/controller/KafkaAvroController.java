package com.example.KafkaSetupAvroSchema.controller;

import com.example.KafkaSetupAvroSchema.model.Student;
import com.example.KafkaSetupAvroSchema.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class KafkaAvroController {
    @Autowired
    ProducerService producerService;

    @PostMapping(value = "/send/avro/student/info")
    public String kafkaMessage(@RequestBody Student message) {
        producerService.sendMessage(message);
        return "Success";
    }
}
