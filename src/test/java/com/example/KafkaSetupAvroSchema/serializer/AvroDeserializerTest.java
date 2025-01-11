package com.example.KafkaSetupAvroSchema.serializer;

import com.example.KafkaSetupAvroSchema.model.Student;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class AvroDeserializerTest {
    private AvroDeserializer<Student> avroDeserializer;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        avroDeserializer = new AvroDeserializer<>(Student.class);
    }

    @Test
    void testCloseWithoutFail() {
        assertDoesNotThrow(()-> avroDeserializer.close());
    }

    @Test
    void testDeserializeReturnsNullWhenBytesAreNull() {
        assertNull(avroDeserializer.deserialize("testTopic", null));
    }

    @Test
    void testDeserializeHandlesException() throws IOException {
        Student student = new Student("John Doe", "12345", 20);
        byte[] serializedBytes = getStudentData(student);

        // Deserialize the bytes back to a Student object
        Student deserializedStudent = avroDeserializer.deserialize("test-topic", serializedBytes);

        assertNotNull(deserializedStudent);
        assertEquals("John Doe", deserializedStudent.getStudentName().toString());
        assertEquals("12345", deserializedStudent.getStudentId().toString());
        assertEquals(20, deserializedStudent.getAge());
    }

    @Test
    void testExceptionHandlingDuringDeserialization() throws Exception {
        // Mock the DatumReader to throw an exception
        Student student = new Student("John Doe", "12345", 20);
        byte[] serializedBytes = getStudentData(student);

        // Simulate exception handling
        byte[] truncatedBytes = Arrays.copyOf(serializedBytes, serializedBytes.length - 1);

        Student deserializedStudent = avroDeserializer.deserialize("test-topic", truncatedBytes);
        assertNull(deserializedStudent);
    }

    byte[] getStudentData(Student payload) throws IOException {
        byte[] bytes = null;
        try {
            if (payload != null) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(payload.getSchema());
                datumWriter.write(payload, binaryEncoder);
                binaryEncoder.flush();
                byteArrayOutputStream.close();
                bytes = byteArrayOutputStream.toByteArray();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bytes;
    }
}