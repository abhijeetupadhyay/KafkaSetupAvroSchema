package com.example.KafkaSetupAvroSchema.serializer;

import com.example.KafkaSetupAvroSchema.model.Student;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class AvroSerializerTest {
    @Test
    void testSerializeValidPayload() {
        // Arrange
        AvroSerializer<SpecificRecordBase> serializer = new AvroSerializer<>();
        Student student = new Student("John Doe", "12345", 20);

        // Act
        byte[] serializedBytes = serializer.serialize("test-topic", student);

        // Assert
        assertNotNull(serializedBytes);
        assertTrue(serializedBytes.length > 0);
    }

    @Test
    void testSerializeNullPayload() {
        // Arrange
        AvroSerializer<SpecificRecordBase> serializer = new AvroSerializer<>();

        // Act
        byte[] serializedBytes = serializer.serialize("test-topic", null);

        // Assert
        assertNull(serializedBytes);
    }

//    @Test
//    void testSerializeHandlesException() {
//        // Arrange
//        AvroSerializer<SpecificRecordBase> serializer = new AvroSerializer<>();
//
//        // Passing an invalid object (not a valid Avro schema)
//        Object invalidObject = new Object();
//
//        // Act
//        byte[] serializedBytes = serializer.serialize("test-topic", (SpecificRecordBase) invalidObject);
//
//        // Assert
//        assertNull(serializedBytes); // Ensure no data is returned
//    }

    @Test
    void testConfigureAndClose() {
        // Arrange
        AvroSerializer<SpecificRecordBase> serializer = new AvroSerializer<>();

        // Act & Assert
        assertDoesNotThrow(() -> serializer.configure(Map.of(), false));
        assertDoesNotThrow(serializer::close);
    }
}
