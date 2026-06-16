# Spec: Kafka Header Contract

**Version**: 1  
**Status**: Draft  
**Owner**: ktrace-core

## Purpose

Define the exact Kafka header names and values used to propagate trace context across services. This contract ensures `ktrace` headers never collide with standard framework headers and remain parseable by all ktrace components.

---

## Header Namespace

All ktrace headers use the **`ktrace-` prefix** (lowercase, hyphenated).

### Reserved Namespaces (MUST NOT USE)

- `kafka_*` — Spring Kafka internal headers
- `traceparent`, `tracestate`, `baggage` — W3C OpenTelemetry
- `X-B3-*` — Zipkin B3 propagation
- `uber-trace-id` — Jaeger

---

## Header Definitions

| Header Key | Value Type | Required | Description |
|------------|------------|----------|-------------|
| `ktrace-trace-id` | String (UUID v4) | Yes | Top-level causal chain identifier |
| `ktrace-span-id` | String (UUID v4) | Yes | This specific produce call |
| `ktrace-parent-span-id` | String (UUID v4) | No | Triggering span (absent for root) |
| `ktrace-trigger-topic` | String | No | Topic of the consumer record that triggered this produce |
| `ktrace-trigger-partition` | String (int) | No | Partition of trigger record |
| `ktrace-trigger-offset` | String (long) | No | Offset of trigger record |
| `ktrace-trigger-group` | String | No | Consumer group of trigger record |
| `ktrace-app-name` | String | No | Application name (from config) |

### Value Encoding

- All values are UTF-8 strings
- Numeric values (partition, offset) are serialized as decimal strings (e.g., `"42"`, `"1000"`)
- UUIDs are lowercase with hyphens (e.g., `"550e8400-e29b-41d4-a716-446655440000"`)

---

## Scenario 1: Producer Injects Root Headers

**Given** a producer sends a message with no incoming trigger context

**When** `KTraceProducerInterceptor.onSend()` is called

**Then** the following headers must be added to the `ProducerRecord`:

```
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
ktrace-app-name: order-service
```

**And** `ktrace-parent-span-id` must NOT be present  
**And** `ktrace-trigger-*` headers must NOT be present  
**And** existing headers (user-defined or framework) must be preserved

---

## Scenario 2: Consumer Extracts and Propagates

**Given** a consumer receives a `ConsumerRecord` with headers:
```
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
kafka_offset: 100
kafka_receivedTopic: orders
```

**When** `TraceContextPropagator.extractAndBind(record)` is called

**Then** a `TraceContext` must be created with:
- `traceId = "550e8400-e29b-41d4-a716-446655440000"`
- `spanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"` (becomes the parent for next produce)
- `parentSpanId = null`

**And** when the consumer produces to a downstream topic, the headers must be:
```
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 7c9e6679-7425-40de-944b-e07fc1f90ae7  (NEW)
ktrace-parent-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
ktrace-trigger-topic: orders
ktrace-trigger-partition: 2
ktrace-trigger-offset: 100
ktrace-trigger-group: order-processor-group
```

**And** `ktrace-trace-id` must match the incoming message (chain preserved)  
**And** `ktrace-span-id` must be a new UUID  
**And** `ktrace-parent-span-id` must be the incoming `ktrace-span-id`

---

## Scenario 3: Header Collision Detection

**Given** a Kafka record already has a header named `ktrace-trace-id`  
**And** a producer attempts to send it via `KTraceProducerInterceptor`

**When** `onSend()` is called

**Then** the interceptor must:
- Log a warning: "ktrace-trace-id header already present, overwriting"
- Replace the existing header with the new trace context
- NOT throw an exception (graceful overwrite)

---

## Scenario 4: Missing Headers (Incomplete Context)

**Given** a consumer receives a record with only `ktrace-trace-id` header (missing `ktrace-span-id`)

**When** `TraceContextPropagator.extract(headers)` is called

**Then** it must:
- Return `TraceContext` with `traceId` populated
- Generate a new `spanId` (treat as root despite having traceId)
- Log a warning: "Incomplete ktrace headers, treating as root span"

---

## Scenario 5: Header Injection Preserves User Headers

**Given** a `ProducerRecord` with user-defined headers:
```
X-Request-ID: req-abc123
X-User-ID: user-456
```

**When** `KTraceProducerInterceptor.onSend()` adds ktrace headers

**Then** the final headers must be:
```
X-Request-ID: req-abc123
X-User-ID: user-456
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
ktrace-app-name: order-service
```

**And** user headers must NOT be modified or removed  
**And** header order is not guaranteed (Kafka `Headers` is unordered)

---

## Scenario 6: Numeric Header Values

**Given** trigger metadata with partition=3 and offset=5000000000 (long)

**When** headers are injected

**Then** they must be serialized as decimal strings:
```
ktrace-trigger-partition: 3
ktrace-trigger-offset: 5000000000
```

**And** when extracted, they must parse back to `int` and `long` correctly

---

## Scenario 7: No Header Collision with Spring Kafka

**Given** a Spring Boot app uses `@KafkaListener` with Spring Kafka internal headers

**When** ktrace is enabled

**Then** the following Spring headers must remain intact:
```
kafka_offset: 100
kafka_receivedTopic: orders
kafka_receivedPartitionId: 2
kafka_receivedTimestamp: 1718582400000
kafka_consumer: org.apache.kafka.clients.consumer.KafkaConsumer@1234
```

**And** ktrace headers must NOT overwrite or interfere with `kafka_*` headers  
**And** Spring Kafka message listeners must receive all headers

---

## Scenario 8: No Header Collision with OpenTelemetry

**Given** an application also uses OpenTelemetry Java agent with W3C propagation

**When** a record has both OTel and ktrace headers:
```
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
tracestate: rojo=00f067aa0ba902b7
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

**Then** both systems must coexist:
- OTel reads `traceparent`/`tracestate` (ignores `ktrace-*`)
- ktrace reads `ktrace-*` (ignores `traceparent`/`tracestate`)
- Neither corrupts the other's context

---

## Acceptance Criteria

- [ ] All header keys start with `ktrace-` prefix
- [ ] Header keys are lowercase with hyphens (not camelCase or snake_case)
- [ ] `ktrace-trace-id` and `ktrace-span-id` are always present (required headers)
- [ ] `ktrace-parent-span-id` is absent for root spans
- [ ] Numeric values are serialized as decimal strings
- [ ] UUID values are lowercase with hyphens
- [ ] `TraceContextPropagator.inject()` preserves existing headers
- [ ] `TraceContextPropagator.extract()` handles missing/incomplete headers gracefully
- [ ] No collision with `kafka_*`, `traceparent`, `X-B3-*`, or `uber-trace-id` headers
- [ ] Header extraction is case-insensitive (handle `KTrace-Trace-ID` same as `ktrace-trace-id`)

---

## Test Examples

### Java Test (JUnit 5)

```java
@Test
void inject_shouldAddRequiredHeaders() {
    TraceContext ctx = TraceContext.root("550e8400-e29b-41d4-a716-446655440000");
    Headers headers = new RecordHeaders();
    
    TraceContextPropagator.inject(ctx, headers);
    
    assertThat(headers.lastHeader("ktrace-trace-id").value())
        .asString(UTF_8)
        .isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    assertThat(headers.lastHeader("ktrace-span-id").value())
        .asString(UTF_8)
        .matches("^[a-f0-9-]{36}$");
    assertThat(headers.lastHeader("ktrace-parent-span-id")).isNull();
}

@Test
void inject_shouldPreserveUserHeaders() {
    TraceContext ctx = TraceContext.root("550e8400-e29b-41d4-a716-446655440000");
    Headers headers = new RecordHeaders();
    headers.add("X-Request-ID", "req-123".getBytes(UTF_8));
    
    TraceContextPropagator.inject(ctx, headers);
    
    assertThat(headers.lastHeader("X-Request-ID").value())
        .asString(UTF_8)
        .isEqualTo("req-123");
    assertThat(headers.headers("X-Request-ID")).hasSize(1);  // not duplicated
}

@Test
void extract_shouldHandleMissingHeaders() {
    Headers headers = new RecordHeaders();
    // No ktrace headers at all
    
    TraceContext ctx = TraceContextPropagator.extract(headers);
    
    assertThat(ctx).isNull();  // or returns a new root context, depending on design
}

@Test
void extract_shouldHandleIncompleteHeaders() {
    Headers headers = new RecordHeaders();
    headers.add("ktrace-trace-id", "550e8400-e29b-41d4-a716-446655440000".getBytes(UTF_8));
    // Missing ktrace-span-id
    
    TraceContext ctx = TraceContextPropagator.extract(headers);
    
    assertThat(ctx.getTraceId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    assertThat(ctx.getSpanId()).isNotNull();  // generated
}

@Test
void noCollision_withSpringKafkaHeaders() {
    Headers headers = new RecordHeaders();
    headers.add("kafka_offset", "100".getBytes(UTF_8));
    headers.add("kafka_receivedTopic", "orders".getBytes(UTF_8));
    
    TraceContext ctx = TraceContext.root("550e8400-e29b-41d4-a716-446655440000");
    TraceContextPropagator.inject(ctx, headers);
    
    assertThat(headers.lastHeader("kafka_offset").value())
        .asString(UTF_8)
        .isEqualTo("100");  // unchanged
    assertThat(headers.headers("ktrace-trace-id")).hasSize(1);
}
```

---

## Related Specs

- [TraceEvent Schema](trace-event-schema.spec.md) — defines the event published to `__ktrace`
- [MDC Lifecycle](mdc-lifecycle.spec.md) — defines how headers map to SLF4J MDC
