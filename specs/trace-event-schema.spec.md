# Spec: TraceEvent Schema

**Version**: 1  
**Status**: Draft  
**Owner**: ktrace-core

## Purpose

Define the canonical schema for `TraceEvent` objects published to the `__ktrace` topic. This schema is the foundation of causality reconstruction — every field must be present, typed correctly, and serializable to JSON.

---

## Schema Definition

A `TraceEvent` is an immutable value object with exactly these fields:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `traceId` | String (UUID v4) | No | Top-level causal chain identifier |
| `spanId` | String (UUID v4) | No | This specific produce call |
| `parentSpanId` | String (UUID v4) | Yes | Triggering span (null for root) |
| `producedTopic` | String | No | Topic the traced message was sent to |
| `producedPartition` | int | No | Partition (-1 before ack) |
| `producedOffset` | long | No | Offset (-1 before ack) |
| `triggerTopic` | String | Yes | Topic of trigger consumer record |
| `triggerPartition` | int | No | Partition of trigger (-1 if none) |
| `triggerOffset` | long | No | Offset of trigger (-1 if none) |
| `triggerConsumerGroup` | String | Yes | Consumer group of trigger |
| `producerTimestampMs` | long | No | System.currentTimeMillis() at intercept |
| `clientId` | String | No | Kafka producer client.id |
| `applicationName` | String | Yes | From config (optional) |
| `messageKey` | String | Yes | toString() of key, max 256 chars |
| `messageSizeBytes` | int | No | Serialized value size |
| `schemaVersion` | int | No | Currently 1 |

---

## Scenario 1: Root Event (No Trigger)

**Given** a producer sends a message with no incoming trigger context

**When** the `TraceEvent` is created

**Then** it should have:
```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "spanId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "parentSpanId": null,
  "producedTopic": "orders",
  "producedPartition": -1,
  "producedOffset": -1,
  "triggerTopic": null,
  "triggerPartition": -1,
  "triggerOffset": -1,
  "triggerConsumerGroup": null,
  "producerTimestampMs": 1718582400000,
  "clientId": "producer-1",
  "applicationName": "order-service",
  "messageKey": "order-123",
  "messageSizeBytes": 256,
  "schemaVersion": 1
}
```

**And** `traceId` must be a valid UUID v4  
**And** `spanId` must be a valid UUID v4  
**And** `parentSpanId` must be null  
**And** `producedPartition` must be -1 (not yet assigned)  
**And** `producedOffset` must be -1 (not yet assigned)

---

## Scenario 2: Child Event (With Trigger)

**Given** a consumer receives a message with `traceId=550e8400-e29b-41d4-a716-446655440000` from `orders` topic, partition 2, offset 100  
**And** the consumer produces a new message to `notifications` topic

**When** the `TraceEvent` is created for the outgoing message

**Then** it should have:
```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "spanId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "parentSpanId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "producedTopic": "notifications",
  "producedPartition": -1,
  "producedOffset": -1,
  "triggerTopic": "orders",
  "triggerPartition": 2,
  "triggerOffset": 100,
  "triggerConsumerGroup": "order-processor-group",
  "producerTimestampMs": 1718582401000,
  "applicationName": "notification-service",
  "messageKey": "notification-456",
  "messageSizeBytes": 128,
  "schemaVersion": 1
}
```

**And** `traceId` must match the incoming message's `traceId` (chain preserved)  
**And** `spanId` must be a new UUID v4 (not the parent's)  
**And** `parentSpanId` must be the incoming message's `spanId`  
**And** `triggerTopic` must be "orders"  
**And** `triggerPartition` must be 2  
**And** `triggerOffset` must be 100  
**And** `triggerConsumerGroup` must be "order-processor-group"

---

## Scenario 3: Patch Event (After Acknowledgement)

**Given** a `TraceEvent` was published with `producedPartition=-1` and `producedOffset=-1`  
**And** the Kafka broker acknowledges the message with partition=3, offset=500

**When** the interceptor publishes a "patch" event

**Then** it should have:
```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "spanId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "parentSpanId": null,
  "producedTopic": "orders",
  "producedPartition": 3,
  "producedOffset": 500,
  "triggerTopic": null,
  "triggerPartition": -1,
  "triggerOffset": -1,
  "triggerConsumerGroup": null,
  "producerTimestampMs": 1718582400000,
  "clientId": "producer-1",
  "applicationName": "order-service",
  "messageKey": "order-123",
  "messageSizeBytes": 256,
  "schemaVersion": 1
}
```

**And** `spanId` must match the original event's `spanId` (same span, updated metadata)  
**And** `producedPartition` must be 3 (resolved)  
**And** `producedOffset` must be 500 (resolved)

---

## Scenario 4: JSON Serialization Round-Trip

**Given** a `TraceEvent` object with all fields populated

**When** it is serialized to JSON via `TraceEventSerializer`  
**And** deserialized back via `TraceEventDeserializer`

**Then** the deserialized object must equal the original object (all fields match)  
**And** no information must be lost  
**And** null fields must remain null (not empty strings or zero)

---

## Scenario 5: Message Key Truncation

**Given** a Kafka record with a key longer than 256 characters  
**When** the `TraceEvent` is created  
**Then** `messageKey` field must be truncated to exactly 256 characters  
**And** the truncation should preserve the beginning of the key

---

## Scenario 6: Schema Version Forward Compatibility

**Given** a `TraceEvent` with `schemaVersion=1`  
**When** a future version adds a new field (e.g., `samplingProbability`)  
**Then** old consumers (reading v1 schema) must ignore unknown fields  
**And** must not fail deserialization  
**And** must successfully extract all v1 fields

---

## Acceptance Criteria

- [x] `TraceEvent` class is immutable (all fields `final`, no setters)
- [x] All non-nullable fields throw `NullPointerException` if null passed to constructor
- [x] `traceId` and `spanId` are validated as UUID v4 format
- [x] `producedPartition` and `producedOffset` default to -1 when unknown
- [x] `TraceEventSerializer` produces valid JSON matching schema
- [x] `TraceEventDeserializer` handles null fields correctly
- [x] Round-trip serialization preserves all field values
- [ ] `messageKey` is truncated to 256 characters if longer
- [x] `schemaVersion` field is always set to 1
- [ ] Kafka record key (published to `__ktrace`) is set to `traceId`

---

## Non-Requirements

- This spec does NOT define the `__ktrace` topic configuration (partitions, replication, retention)
- This spec does NOT define how `TraceEvent` objects are consumed or reassembled into chains (see `causal-chain.spec.md`)
- This spec does NOT define Avro or Protobuf serialization (JSON only for v1)

---

## Test Examples

### Java Test (JUnit 5)

```java
@Test
void rootEvent_shouldHaveNullParentAndTrigger() {
    TraceEvent event = TraceEvent.builder()
        .traceId(UUID.randomUUID().toString())
        .spanId(UUID.randomUUID().toString())
        .parentSpanId(null)
        .producedTopic("orders")
        .producedPartition(-1)
        .producedOffset(-1)
        .triggerTopic(null)
        .triggerPartition(-1)
        .triggerOffset(-1)
        .triggerConsumerGroup(null)
        .producerTimestampMs(System.currentTimeMillis())
        .clientId("producer-1")
        .applicationName("order-service")
        .messageKey("order-123")
        .messageSizeBytes(256)
        .schemaVersion(1)
        .build();

    assertThat(event.getParentSpanId()).isNull();
    assertThat(event.getTriggerTopic()).isNull();
    assertThat(event.getProducedPartition()).isEqualTo(-1);
    assertThat(event.getProducedOffset()).isEqualTo(-1);
}

@Test
void serialization_shouldRoundTrip() throws Exception {
    TraceEvent original = createSampleEvent();
    
    byte[] json = new TraceEventSerializer().serialize("__ktrace", original);
    TraceEvent deserialized = new TraceEventDeserializer().deserialize("__ktrace", json);
    
    assertThat(deserialized).isEqualTo(original);
}

@Test
void messageKey_shouldTruncateTo256Chars() {
    String longKey = "a".repeat(300);
    
    TraceEvent event = TraceEvent.builder()
        .messageKey(longKey)
        // ... other fields
        .build();
    
    assertThat(event.getMessageKey()).hasSize(256);
    assertThat(event.getMessageKey()).startsWith("aaa");
}
```

---

## Related Specs

- [Kafka Header Contract](kafka-headers.spec.md) — defines how trace context is propagated via Kafka headers
- [Causal Chain Reconstruction](causal-chain.spec.md) — defines how `TraceEvent` objects are assembled into DAGs
