package io.ktrace.core.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * Kafka deserializer for TraceEvent objects.
 * <p>
 * Converts JSON bytes from the __ktrace topic back into TraceEvent instances.
 * Uses Jackson to parse JSON and reconstruct immutable TraceEvent objects.
 */
public class TraceEventDeserializer implements Deserializer<TraceEvent> {

    private final ObjectMapper mapper;

    public TraceEventDeserializer() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public TraceEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.readValue(data, TraceEvent.class);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize TraceEvent", e);
        }
    }
}
