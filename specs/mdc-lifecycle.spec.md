# Spec: MDC Lifecycle

**Version**: 1  
**Status**: Draft  
**Owner**: ktrace-core

## Purpose

Define how trace context is injected into SLF4J MDC (Mapped Diagnostic Context) so application logs automatically include trace IDs. The lifecycle management ensures ThreadLocal MDC is set during produce/consume and cleanly cleared afterward to prevent memory leaks and context pollution.

---

## MDC Keys

ktrace sets exactly 3 keys in `org.slf4j.MDC`:

| MDC Key | Value | Present When |
|---------|-------|--------------|
| `ktrace.traceId` | String (UUID v4) | Always during active span |
| `ktrace.spanId` | String (UUID v4) | Always during active span |
| `ktrace.parentSpanId` | String (UUID v4) or null | Only if span has a parent |

### MDC Key Namespace

- Prefix: `ktrace.` (lowercase, dot-separated)
- NO collision with common MDC keys: `requestId`, `userId`, `sessionId`, `traceId` (no prefix), `X-*`

---

## Scenario 1: Producer Sets MDC on Send

**Given** a producer sends a message via `KafkaProducer.send()`  
**And** `KTraceProducerInterceptor` is configured

**When** `onSend()` is called

**Then** MDC must be set with:
```
ktrace.traceId = "550e8400-e29b-41d4-a716-446655440000"
ktrace.spanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
```

**And** `ktrace.parentSpanId` is NOT set (absent from MDC) if this is a root span

**And** subsequent log statements in the same thread must include these keys:
```
2026-06-16 10:30:00.123 INFO [ktrace.traceId=550e8400-e29b-41d4-a716-446655440000, ktrace.spanId=6ba7b810-9dad-11d1-80b4-00c04fd430c8] Sending order order-123
```

---

## Scenario 2: Producer Clears MDC After Acknowledgement

**Given** MDC was set during `onSend()`  
**And** the Kafka broker acknowledges the message

**When** `onAcknowledgement()` is called

**Then** `KTraceMDC.clear()` must be invoked  
**And** `MDC.get("ktrace.traceId")` must return `null`  
**And** `MDC.get("ktrace.spanId")` must return `null`  
**And** `MDC.get("ktrace.parentSpanId")` must return `null`

**And** other MDC keys (user-defined or framework) must remain intact:
```
// Before clear():
MDC: {ktrace.traceId=..., ktrace.spanId=..., requestId=req-123, userId=user-456}

// After KTraceMDC.clear():
MDC: {requestId=req-123, userId=user-456}
```

---

## Scenario 3: Consumer Sets MDC on Receive

**Given** a consumer receives a `ConsumerRecord` with headers:
```
ktrace-trace-id: 550e8400-e29b-41d4-a716-446655440000
ktrace-span-id: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
ktrace-parent-span-id: abc12345-1234-1234-1234-123456789abc
```

**When** `TraceContextPropagator.extractAndBind(record)` is called

**Then** MDC must be set with:
```
ktrace.traceId = "550e8400-e29b-41d4-a716-446655440000"
ktrace.spanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
ktrace.parentSpanId = "abc12345-1234-1234-1234-123456789abc"
```

**And** the consumer's log statements must include these keys:
```
2026-06-16 10:30:01.456 INFO [ktrace.traceId=550e8400-e29b-41d4-a716-446655440000, ktrace.spanId=6ba7b810-9dad-11d1-80b4-00c04fd430c8, ktrace.parentSpanId=abc12345-1234-1234-1234-123456789abc] Processing order order-123
```

---

## Scenario 4: Consumer Clears MDC in Finally Block

**Given** a consumer has set MDC via `extractAndBind()`  
**And** the consumer processes the record (successfully or with exception)

**When** the consumer's processing completes (or fails)

**Then** the application code must call `KTraceMDC.clear()` in a `finally` block:
```java
try {
    TraceContextPropagator.extractAndBind(record);
    processRecord(record);
} finally {
    KTraceMDC.clear();
}
```

**And** after `clear()`, MDC must NOT contain `ktrace.*` keys  
**And** this must happen even if `processRecord()` throws an exception

---

## Scenario 5: Spring Boot Auto-Clears MDC for @KafkaListener

**Given** a Spring Boot app with `ktrace-spring` dependency  
**And** a `@KafkaListener` method

**When** the listener is invoked

**Then** `KTraceMDCKafkaListenerInterceptor` must:
1. Call `extractAndBind(record)` BEFORE listener invocation
2. Call `KTraceMDC.clear()` AFTER listener completes (success or failure)

**And** the user does NOT need to manually call `clear()` (zero-config)

**And** logs inside the listener must show `ktrace.*` keys:
```java
@KafkaListener(topics = "orders")
public void listen(ConsumerRecord<String, String> record) {
    log.info("Processing {}", record.key());  // ktrace.traceId present in MDC
}
// After method returns, ktrace.* keys automatically cleared
```

---

## Scenario 6: Scoped MDC Helper

**Given** a reactive/async code path where ThreadLocal is unreliable

**When** using `KTraceMDC.scoped()` helper

**Then** MDC must be set, code executed, and MDC cleared in try-finally:
```java
KTraceMDC.scoped(context, () -> {
    log.info("Inside scoped block");  // ktrace.* keys present
});
// After lambda completes, ktrace.* keys automatically cleared
```

**And** if the lambda throws an exception, `clear()` must still be called

---

## Scenario 7: No MDC Leak Across Threads

**Given** a producer sends a message on Thread-A  
**And** MDC is set on Thread-A

**When** a background thread (Thread-B) runs asynchronously

**Then** Thread-B's MDC must NOT contain `ktrace.*` keys (ThreadLocal isolation)  
**And** Thread-A's MDC must remain intact until `clear()` is called on Thread-A

---

## Scenario 8: No MDC Leak After Tests

**Given** a JUnit test that produces/consumes messages  
**And** MDC is set during the test

**When** the test completes (success or failure)

**Then** `@AfterEach` must verify MDC is clean:
```java
@AfterEach
void verifyNoMDCLeak() {
    assertThat(MDC.get("ktrace.traceId")).isNull();
    assertThat(MDC.get("ktrace.spanId")).isNull();
    assertThat(MDC.get("ktrace.parentSpanId")).isNull();
}
```

**And** if MDC is not cleared, the test must fail with a clear error message

---

## Scenario 9: MDC Clear Preserves Other Keys

**Given** MDC contains:
```
ktrace.traceId = "550e8400-e29b-41d4-a716-446655440000"
ktrace.spanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
requestId = "req-abc123"
userId = "user-456"
sessionId = "session-789"
```

**When** `KTraceMDC.clear()` is called

**Then** MDC must contain:
```
requestId = "req-abc123"
userId = "user-456"
sessionId = "session-789"
```

**And** `ktrace.*` keys must be removed  
**And** NO other keys must be removed (selective clear, not `MDC.clear()`)

---

## Scenario 10: MDC Current Reads Back Context

**Given** MDC contains `ktrace.traceId` and `ktrace.spanId`

**When** `KTraceMDC.current()` is called

**Then** it must return a `TraceContext` with:
- `traceId` from `MDC.get("ktrace.traceId")`
- `spanId` from `MDC.get("ktrace.spanId")`
- `parentSpanId` from `MDC.get("ktrace.parentSpanId")` (null if absent)

**And** this is useful for cross-thread hand-off:
```java
TraceContext ctx = KTraceMDC.current();  // Thread-A
executor.submit(() -> {
    KTraceMDC.put(ctx);  // Thread-B
    // ... work with trace context
    KTraceMDC.clear();
});
```

---

## Acceptance Criteria

- [ ] `KTraceMDC.put(TraceContext)` sets exactly 3 MDC keys (or 2 if no parent)
- [ ] `KTraceMDC.clear()` removes only `ktrace.*` keys, preserves others
- [ ] `KTraceMDC.current()` reads back `TraceContext` from MDC
- [ ] `KTraceMDC.scoped(context, runnable)` provides try-finally safety
- [ ] Producer interceptor sets MDC in `onSend()`, clears in `onAcknowledgement()`
- [ ] Consumer `extractAndBind()` sets MDC, user calls `clear()` in finally block
- [ ] Spring Boot `@KafkaListener` auto-clears MDC (zero-config)
- [ ] No MDC leak across threads (ThreadLocal isolation)
- [ ] No MDC leak after tests (verified via `@AfterEach`)
- [ ] Log statements show `ktrace.traceId`, `ktrace.spanId`, `ktrace.parentSpanId`

---

## Test Examples

### Java Test (JUnit 5)

```java
@Test
void put_shouldSetMDCKeys() {
    TraceContext ctx = new TraceContext(
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        null
    );
    
    KTraceMDC.put(ctx);
    
    assertThat(MDC.get("ktrace.traceId")).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    assertThat(MDC.get("ktrace.spanId")).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    assertThat(MDC.get("ktrace.parentSpanId")).isNull();
    
    KTraceMDC.clear();  // cleanup
}

@Test
void clear_shouldRemoveOnlyKTraceKeys() {
    MDC.put("ktrace.traceId", "550e8400-e29b-41d4-a716-446655440000");
    MDC.put("ktrace.spanId", "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    MDC.put("requestId", "req-123");
    MDC.put("userId", "user-456");
    
    KTraceMDC.clear();
    
    assertThat(MDC.get("ktrace.traceId")).isNull();
    assertThat(MDC.get("ktrace.spanId")).isNull();
    assertThat(MDC.get("requestId")).isEqualTo("req-123");
    assertThat(MDC.get("userId")).isEqualTo("user-456");
}

@Test
void scoped_shouldClearAfterRunnable() {
    TraceContext ctx = new TraceContext(
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        null
    );
    
    KTraceMDC.scoped(ctx, () -> {
        assertThat(MDC.get("ktrace.traceId")).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    });
    
    assertThat(MDC.get("ktrace.traceId")).isNull();  // cleared after lambda
}

@Test
void scoped_shouldClearEvenOnException() {
    TraceContext ctx = new TraceContext(
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        null
    );
    
    assertThatThrownBy(() ->
        KTraceMDC.scoped(ctx, () -> {
            throw new RuntimeException("test");
        })
    ).isInstanceOf(RuntimeException.class);
    
    assertThat(MDC.get("ktrace.traceId")).isNull();  // still cleared
}

@AfterEach
void verifyNoMDCLeak() {
    assertThat(MDC.get("ktrace.traceId")).isNull();
    assertThat(MDC.get("ktrace.spanId")).isNull();
    assertThat(MDC.get("ktrace.parentSpanId")).isNull();
}
```

### Logback Configuration

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %level [%X{ktrace.traceId:-}, %X{ktrace.spanId:-}, %X{ktrace.parentSpanId:-}] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="CONSOLE" />
    </root>
</configuration>
```

Expected log output:
```
2026-06-16 10:30:00.123 INFO [550e8400-e29b-41d4-a716-446655440000, 6ba7b810-9dad-11d1-80b4-00c04fd430c8, -] io.ktrace.examples.VanillaProducerApp - Sending order order-123
```

---

## Related Specs

- [Kafka Header Contract](kafka-headers.spec.md) — how trace context is propagated via headers
- [Interceptor Behavior](interceptor-behavior.spec.md) — when MDC is set/cleared during produce
