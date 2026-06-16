# Spec: Interceptor Behavior

**Version**: 1  
**Status**: Draft  
**Owner**: ktrace-core

## Purpose

Define the complete lifecycle behavior of `KTraceProducerInterceptor`, the zero-config entry point for ktrace. This interceptor wraps every `KafkaProducer.send()` call, extracts/injects trace context, publishes `TraceEvent` objects, and manages MDC lifecycle — all transparently without application code changes.

---

## Configuration

### Kafka Producer Properties

```properties
# Required: register the interceptor
interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor

# Optional ktrace config (with ktrace. prefix)
ktrace.enabled=true
ktrace.trace-topic=__ktrace
ktrace.async-queue-size=1000
ktrace.application-name=order-service
```

### KTraceProducerInterceptor.configure()

**When** `KafkaProducer` is instantiated  
**Then** `configure(Map<String, ?> configs)` is called once with producer config  
**And** the interceptor must:
1. Read all `ktrace.*` properties and build `KTraceConfig`
2. Create `AsyncTracePublisher` (lazy init, shared across sends)
3. Extract `client.id` from config (used in `TraceEvent`)

---

## Scenario 1: First Produce (Root Span, No Trigger)

**Given** a producer sends a message:
```java
ProducerRecord<String, Order> record = new ProducerRecord<>("orders", "order-123", order);
producer.send(record);
```

**And** there is NO trace context in `TraceContext.current()` (ThreadLocal empty)

**When** `onSend(ProducerRecord<K,V> record)` is called

**Then** the interceptor must:
1. Call `TraceContextPropagator.extract(record.headers())` — returns `null` (no incoming context)
2. Generate a new `traceId` (UUID v4)
3. Generate a new `spanId` (UUID v4)
4. Set `parentSpanId = null` (root span)
5. Create `TraceContext` with (traceId, spanId, null)
6. Call `KTraceMDC.put(context)` — sets MDC keys
7. Call `TraceContextPropagator.inject(context, record.headers())` — adds `ktrace-*` headers
8. Build `TraceEvent` with:
   - `traceId`, `spanId`, `parentSpanId=null`
   - `producedTopic="orders"`
   - `producedPartition=-1`, `producedOffset=-1` (not yet known)
   - `triggerTopic=null`, `triggerPartition=-1`, `triggerOffset=-1` (no trigger)
   - `producerTimestampMs=System.currentTimeMillis()`
   - `clientId="producer-1"` (from config)
   - `applicationName="order-service"` (from config)
   - `messageKey="order-123"`
   - `messageSizeBytes=<serialized size>` (estimated from value serializer)
   - `schemaVersion=1`
9. Call `AsyncTracePublisher.publish(event)` — enqueues event (non-blocking)
10. Return the modified `ProducerRecord` (with injected headers)

**And** MDC must contain `ktrace.traceId` and `ktrace.spanId`  
**And** the `TraceEvent` must be enqueued to `__ktrace` topic (fire-and-forget)  
**And** the application thread must NOT block on Kafka I/O

---

## Scenario 2: Acknowledgement Callback (Patch Event)

**Given** a message was sent via `onSend()` with `spanId="6ba7b810-9dad-11d1-80b4-00c04fd430c8"`  
**And** the Kafka broker acknowledges the message with `partition=3`, `offset=500`

**When** `onAcknowledgement(RecordMetadata metadata, Exception exception)` is called

**Then** the interceptor must:
1. Build a "patch" `TraceEvent` with the SAME `spanId` but updated:
   - `producedPartition=3`
   - `producedOffset=500`
   - All other fields identical to the original event
2. Call `AsyncTracePublisher.publish(patchEvent)` — enqueues patch event
3. Call `KTraceMDC.clear()` — removes `ktrace.*` MDC keys
4. If `exception != null`, log error but do NOT throw (graceful degradation)

**And** after `clear()`, MDC must NOT contain `ktrace.traceId` or `ktrace.spanId`  
**And** the tracer component can use `spanId` to link the patch event to the original event

---

## Scenario 3: Consumer-Triggered Produce (Child Span)

**Given** a consumer received a message with headers:
```
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
```

**And** the consumer called `TraceContextPropagator.extractAndBind(record)` — set `TraceContext.current()`  
**And** stored trigger metadata:
```
TraceContext.setTrigger("orders", 2, 100, "order-processor-group")
```

**When** the consumer produces a new message to `notifications` topic

**Then** `onSend()` must:
1. Call `extract(record.headers())` — returns the consumer's context (if headers were forwarded, else relies on ThreadLocal)
2. Read `TraceContext.current()` from ThreadLocal — has `traceId="550e8400..."`, `spanId="6ba7b810..."`
3. Generate a NEW `spanId` (e.g., `"7c9e6679-7425-40de-944b-e07fc1f90ae7"`)
4. Set `parentSpanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"` (the consumer's span)
5. Reuse `traceId = "550e8400..."` (chain preserved)
6. Build `TraceEvent` with:
   - `traceId="550e8400..."` (same as parent)
   - `spanId="7c9e6679..."` (new)
   - `parentSpanId="6ba7b810..."` (links to consumer span)
   - `producedTopic="notifications"`
   - `triggerTopic="orders"`, `triggerPartition=2`, `triggerOffset=100`, `triggerConsumerGroup="order-processor-group"`
7. Inject headers into the outgoing record (including `ktrace-parent-span-id`)
8. Publish `TraceEvent`

**And** the causal link is: consumer span `6ba7b810...` → producer span `7c9e6679...`

---

## Scenario 4: Interceptor Chain (Multiple Interceptors)

**Given** a producer is configured with multiple interceptors:
```properties
interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor,com.example.CustomInterceptor
```

**When** `producer.send(record)` is called

**Then** Kafka calls interceptors in order:
1. `KTraceProducerInterceptor.onSend(record)` — adds ktrace headers, publishes event
2. `CustomInterceptor.onSend(record)` — sees ktrace headers already present

**And** `onAcknowledgement()` is called in REVERSE order:
1. `CustomInterceptor.onAcknowledgement(metadata, exception)`
2. `KTraceProducerInterceptor.onAcknowledgement(metadata, exception)` — clears MDC

**And** ktrace must NOT interfere with other interceptors  
**And** header injection must be idempotent (re-running `onSend` produces same result)

---

## Scenario 5: Disabled Tracing

**Given** `ktrace.enabled=false` in producer config

**When** `onSend()` is called

**Then** the interceptor must:
1. Return the record unmodified (no headers added)
2. NOT publish any `TraceEvent`
3. NOT set MDC keys
4. Pass through with zero overhead

**And** logs must NOT contain `ktrace.*` MDC keys

---

## Scenario 6: Queue Overflow (Async Publisher Full)

**Given** `AsyncTracePublisher` has a bounded queue of size 1000  
**And** the queue is full (1000 events waiting)

**When** `onSend()` attempts to publish a new `TraceEvent`

**Then** the interceptor must:
1. Call `AsyncTracePublisher.publish(event)` — returns `false` (queue full)
2. Log a WARNING: "ktrace queue full, dropping event for spanId=..."
3. Continue without blocking (graceful degradation)
4. NOT throw an exception (never fail the application send)

**And** the application's `producer.send()` must succeed  
**And** metrics should track dropped events (future enhancement)

---

## Scenario 7: Serializer Throws Exception

**Given** the `TraceEventSerializer` throws `SerializationException` during `AsyncTracePublisher.publish()`

**When** the background thread attempts to serialize the event

**Then** the publisher must:
1. Log an ERROR: "Failed to serialize TraceEvent for spanId=..., cause=..."
2. Drop the event (do NOT retry, do NOT crash the thread)
3. Continue processing the queue

**And** the application's produce must NOT be affected (fire-and-forget)

---

## Scenario 8: Close Interceptor

**Given** `producer.close()` is called

**When** `KTraceProducerInterceptor.close()` is invoked

**Then** the interceptor must:
1. Call `AsyncTracePublisher.close()` — drains remaining queue, closes internal producer
2. Wait up to 5 seconds for queue to drain
3. If timeout, log WARNING: "ktrace publisher did not drain queue within 5s, force closing"
4. Release all resources (no thread/connection leaks)

**And** after `close()`, no new events can be published

---

## Scenario 9: Thread Safety

**Given** multiple threads concurrently call `producer.send()`

**When** `onSend()` is called from Thread-A and Thread-B simultaneously

**Then** the interceptor must:
1. Use ThreadLocal `TraceContext` to isolate contexts (Thread-A's traceId ≠ Thread-B's traceId)
2. Safely enqueue events to `AsyncTracePublisher` (queue is thread-safe)
3. NOT corrupt MDC (each thread has independent MDC)

**And** no race conditions or lost events

---

## Scenario 10: Idempotent Header Injection

**Given** a `ProducerRecord` already has `ktrace-trace-id` header (from manual injection or retry)

**When** `onSend()` is called

**Then** the interceptor must:
1. Log a WARNING: "ktrace-trace-id already present, overwriting with new context"
2. Replace the existing header with the new trace context
3. Continue normally

**And** the final record has exactly ONE `ktrace-trace-id` header (not duplicated)

---

## Acceptance Criteria

- [ ] `configure()` reads `ktrace.*` properties and initializes `AsyncTracePublisher`
- [ ] `onSend()` extracts context, generates span, injects headers, publishes event, sets MDC
- [ ] `onAcknowledgement()` publishes patch event, clears MDC
- [ ] Root spans have `parentSpanId=null` and no trigger metadata
- [ ] Child spans have `parentSpanId` set to the consumer's `spanId`
- [ ] `traceId` is preserved across causal chain (same traceId from consumer to producer)
- [ ] MDC is set in `onSend()`, cleared in `onAcknowledgement()`
- [ ] Queue overflow does NOT block the application thread
- [ ] Serialization exceptions do NOT crash the background thread
- [ ] `close()` drains the queue and releases resources
- [ ] Thread-safe: concurrent sends from multiple threads work correctly
- [ ] Idempotent: re-running `onSend()` on same record is safe
- [ ] Disabled via `ktrace.enabled=false` has zero overhead

---

## Test Examples

### Java Test (JUnit 5)

```java
@Test
void onSend_rootSpan_shouldInjectHeaders() {
    KTraceProducerInterceptor interceptor = new KTraceProducerInterceptor();
    interceptor.configure(Map.of(
        "client.id", "test-producer",
        "ktrace.enabled", "true",
        "ktrace.application-name", "test-app"
    ));
    
    ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
    ProducerRecord<String, String> modified = interceptor.onSend(record);
    
    assertThat(modified.headers().lastHeader("ktrace-trace-id")).isNotNull();
    assertThat(modified.headers().lastHeader("ktrace-span-id")).isNotNull();
    assertThat(modified.headers().lastHeader("ktrace-parent-span-id")).isNull();
    
    assertThat(MDC.get("ktrace.traceId")).isNotNull();
    assertThat(MDC.get("ktrace.spanId")).isNotNull();
}

@Test
void onAcknowledgement_shouldClearMDC() {
    // Setup: onSend() was called, MDC is set
    MDC.put("ktrace.traceId", "550e8400-e29b-41d4-a716-446655440000");
    MDC.put("ktrace.spanId", "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    
    KTraceProducerInterceptor interceptor = new KTraceProducerInterceptor();
    RecordMetadata metadata = new RecordMetadata(
        new TopicPartition("orders", 3), 500, 0, 1718582400000L, 0L, 5, 100
    );
    
    interceptor.onAcknowledgement(metadata, null);
    
    assertThat(MDC.get("ktrace.traceId")).isNull();
    assertThat(MDC.get("ktrace.spanId")).isNull();
}

@Test
void onSend_childSpan_shouldLinkToParent() {
    TraceContext parentCtx = new TraceContext(
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        null
    );
    TraceContext.setCurrent(parentCtx);
    TraceContext.setTrigger("orders", 2, 100, "consumer-group");
    
    KTraceProducerInterceptor interceptor = new KTraceProducerInterceptor();
    // configure...
    
    ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");
    ProducerRecord<String, String> modified = interceptor.onSend(record);
    
    String traceId = new String(modified.headers().lastHeader("ktrace-trace-id").value(), UTF_8);
    String parentSpanId = new String(modified.headers().lastHeader("ktrace-parent-span-id").value(), UTF_8);
    
    assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
}
```

---

## Related Specs

- [Kafka Header Contract](kafka-headers.spec.md) — header injection/extraction
- [MDC Lifecycle](mdc-lifecycle.spec.md) — when MDC is set/cleared
- [TraceEvent Schema](trace-event-schema.spec.md) — the event published to `__ktrace`
