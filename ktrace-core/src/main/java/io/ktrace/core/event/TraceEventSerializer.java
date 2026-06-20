package io.ktrace.core.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka serializer for TraceEvent objects.
 * <p>
 * Converts TraceEvent instances to JSON bytes for publishing to the __ktrace topic.
 * Uses Jackson with explicit null inclusion to ensure null fields are preserved
 * during round-trip serialization.
 */
public class TraceEventSerializer implements Serializer<TraceEvent> {

    private final ObjectMapper mapper;

    public TraceEventSerializer() {
        this.mapper = new ObjectMapper();
        // Include null values explicitly in JSON
        mapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    }

    @Override
    public byte[] serialize(String topic, TraceEvent event) {
        if (event == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize TraceEvent", e);
        }
    }
}
