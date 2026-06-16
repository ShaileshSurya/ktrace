# ktrace — Kafka Causality Tracer SDK

## What This Is

`ktrace` is a minimalist Java SDK that bridges the causality gap in Kafka-based systems. Whenever a `KafkaProducer` sends a message, the SDK automatically publishes a lightweight `TraceEvent` to a dedicated `__ktrace` topic. A tracer component reads that topic to reconstruct the full causal chain of events across services.

---

## Scope

- **Supported**: plain Java (`ktrace-core`), Spring Boot 3.x (`ktrace-spring`)
- **Out of scope**: Quarkus, Micronaut (can be added later; `ktrace-core` works standalone)
- **Publishable to**: Maven Central under `io.ktrace` group ID

---

## Project Layout

```
ktrace/
├── pom.xml                          ← parent/aggregator POM
│
├── ktrace-bom/
│   └── pom.xml                      ← Bill of Materials (import scope)
│
├── ktrace-core/
│   └── src/main/java/io/ktrace/core/
│       ├── event/
│       │   ├── TraceEvent.java
│       │   └── TraceEventSerializer.java
│       ├── context/
│       │   ├── TraceContext.java
│       │   ├── TraceContextPropagator.java
│       │   └── KTraceMDC.java             ← put/clear ktrace.* MDC keys; scoped() helper
│       ├── interceptor/
│       │   └── KTraceProducerInterceptor.java
│       ├── producer/
│       │   └── KTraceProducer.java
│       ├── publisher/
│       │   ├── TracePublisher.java
│       │   └── AsyncTracePublisher.java
│       ├── config/
│       │   └── KTraceConfig.java
│       └── spi/
│           └── TraceContextCarrier.java
│
├── ktrace-spring/
│   └── src/main/
│       ├── java/io/ktrace/spring/
│       │   ├── KTraceAutoConfiguration.java
│       │   ├── KTraceProperties.java
│       │   ├── KTraceProducerFactoryCustomizer.java
│       │   ├── KTraceMDCKafkaListenerInterceptor.java ← consumer MDC lifecycle
│       │   └── annotation/EnableKafkaTracing.java
│       └── resources/META-INF/spring/
│           └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
│
├── ktrace-tracer/
│   └── src/main/java/io/ktrace/tracer/
│       ├── TraceReader.java
│       ├── CausalNode.java
│       ├── CausalChain.java
│       └── CausalChainBuilder.java
│
├── ktrace-test/
│   └── src/main/java/io/ktrace/test/
│       ├── KTraceEmbeddedKafka.java
│       └── TraceEventAssert.java
│
└── examples/
    ├── docker-compose.yml
    ├── vanilla-kafka/
    │   └── src/main/java/io/ktrace/examples/vanilla/
    │       ├── VanillaProducerApp.java
    │       └── VanillaConsumerApp.java
    └── spring-kafka/
        └── src/main/java/io/ktrace/examples/spring/
            ├── SpringProducerApp.java
            └── SpringConsumerApp.java
```

---

## Kafka Header Names (`ktrace-` prefix — no collision)

| Header | Value |
|---|---|
| `ktrace-trace-id` | UUID v4 — top-level chain identifier |
| `ktrace-span-id` | UUID v4 — this produce call |
| `ktrace-parent-span-id` | UUID v4 — triggering span (nullable) |
| `ktrace-trigger-topic` | topic of the trigger consumer record |
| `ktrace-trigger-partition` | partition of trigger record |
| `ktrace-trigger-offset` | offset of trigger record |
| `ktrace-trigger-group` | consumer group of trigger record |
| `ktrace-app-name` | application name (optional) |

Avoided namespaces: `kafka_*` (Spring internal), `traceparent`/`tracestate`/`baggage` (W3C OTel), `X-B3-*` (Zipkin B3).

---

## TraceEvent Schema (published to `__ktrace`)

```
traceId              String   — UUID v4 (also the Kafka record key)
spanId               String
parentSpanId         String?
producedTopic        String
producedPartition    int      — -1 before ack
producedOffset       long     — -1 before ack
triggerTopic         String?
triggerPartition     int
triggerOffset        long
triggerConsumerGroup String?
producerTimestampMs  long
clientId             String
applicationName      String?
messageKey           String?  — max 256 chars
messageSizeBytes     int
schemaVersion        int      — 1
```

---

## MDC (Log Trace Propagation)

Trace context is injected into SLF4J MDC so that application logs automatically include trace IDs throughout workflows. The SDK manages MDC lifecycle to prevent ThreadLocal leaks.

### MDC Keys

| MDC key | Value |
|---|---|
| `ktrace.traceId` | current traceId |
| `ktrace.spanId` | current spanId |
| `ktrace.parentSpanId` | current parentSpanId (absent if root) |

### Lifecycle

**Producer side (`KTraceProducerInterceptor.onSend`):**
1. Extract context from incoming `ktrace-*` headers → `TraceContext`
2. Generate new `spanId`; inherit or create `traceId`
3. Call `KTraceMDC.put(context)` → sets MDC keys
4. Inject `ktrace-*` headers into `ProducerRecord`
5. Enqueue `TraceEvent`
6. **After produce completes** (`onAcknowledgement` callback): call `KTraceMDC.clear()` → removes only `ktrace.*` keys

**Consumer side (voluntary):**
- `TraceContextPropagator.extractAndBind(ConsumerRecord)` reads incoming headers, sets `TraceContext` + MDC
- Application calls `KTraceMDC.clear()` in `finally` block after processing

### `KTraceMDC` class (in `ktrace-core`)
```java
static void put(TraceContext ctx)      // set ktrace.* MDC keys
static void clear()                    // remove only ktrace.* keys, preserve others
static TraceContext current()          // read back from MDC (cross-thread hand-off)
static void scoped(TraceContext, Runnable) // put → run → clear in try-finally
```

Uses `org.slf4j.MDC` from `slf4j-api`.

### Spring Boot Integration
`ktrace-spring` provides `KTraceMDCKafkaListenerInterceptor` that auto-wires into `@KafkaListener`:
- Calls `TraceContextPropagator.extractAndBind()` before listener invocation
- Calls `KTraceMDC.clear()` after listener returns

Zero-config MDC for both producer and consumer in Spring Boot.

---

## Integration

### Zero-config (interceptor)
```properties
interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor
```
Sets MDC on `onSend()`, clears on `onAcknowledgement()`.

### Explicit wrapper
```java
KTraceProducer<String, Order> p = KTraceProducer.wrap(rawProducer, KTraceConfig.defaults());
```
For reactive/virtual threads where ThreadLocal is unreliable. Use `KTraceMDC.scoped()` helper.

### Spring Boot (auto-configuration)
Add `ktrace-spring` to POM. `KTraceProducerFactoryCustomizer` wires the interceptor; `KTraceMDCKafkaListenerInterceptor` handles consumer MDC lifecycle. No code changes needed.

---

## Implementation Order

1. Parent `pom.xml` + `ktrace-bom`
2. `ktrace-core` — `TraceEvent`, `KTraceConfig`, headers, `KTraceMDC`, `KTraceProducerInterceptor`, `AsyncTracePublisher`
3. `ktrace-test` — JUnit 5 embedded Kafka, `TraceEventAssert`
4. `examples/vanilla-kafka` — PoC: plain Java, causal chain A → B, verify MDC in logs
5. `ktrace-spring` — Spring Boot auto-configuration, `KTraceMDCKafkaListenerInterceptor`
6. `examples/spring-kafka` — PoC: Spring Boot, same causal chain, verify auto MDC lifecycle
7. `ktrace-tracer` — `CausalChainBuilder`, DAG reconstruction

## Verification

- **Unit**: mock `TracePublisher`, assert `TraceEvent` fields + headers + MDC keys set/cleared
- **Integration**: `KTraceEmbeddedKafka` — produce record, assert `TraceEvent` on `__ktrace` within 2s
- **PoC vanilla**: run apps, grep logs for `ktrace.traceId`, assert present during produce, absent after `clear()`
- **PoC Spring**: verify auto-config wired interceptor + consumer interceptor, check MDC in `@KafkaListener` logs
- **Header collision**: grep consumed headers — confirm no `kafka_*`, `traceparent`, `X-B3-*` used
- **MDC leak check**: after each test, assert `MDC.get("ktrace.traceId") == null`

---

## Module Dependency Matrix

| Module | compile | optional/provided |
|---|---|---|
| `ktrace-core` | `kafka-clients`, `slf4j-api` | — |
| `ktrace-spring` | `ktrace-core` | `spring-kafka`, `spring-boot-autoconfigure` |
| `ktrace-tracer` | `ktrace-core` | — |
| `ktrace-test` | `ktrace-core`, `junit-jupiter`, `assertj-core` | — |
| `examples/vanilla-kafka` | `ktrace-core` | — |
| `examples/spring-kafka` | `ktrace-spring` | — |

---

## Maven Central Publishing

Root POM must include: `maven-source-plugin`, `maven-javadoc-plugin`, `maven-gpg-plugin`, `central-publishing-maven-plugin`, `maven-flatten-plugin`, `maven-release-plugin`, plus `<scm>`, `<licenses>` (Apache 2.0), `<developers>`, `<url>`.
