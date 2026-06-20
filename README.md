# ktrace - Kafka Causality Tracing SDK

Minimalist Java SDK for tracing causality chains across Kafka-based systems. Automatically captures producer calls and publishes trace events to reconstruct the full causal chain of events across services.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java Version](https://img.shields.io/badge/java-11%2B-orange.svg)]()

---

## Table of Contents

- [Features](#features)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Installation](#installation)
- [Usage](#usage)
  - [Zero-Config (Interceptor)](#zero-config-interceptor)
  - [Spring Boot](#spring-boot)
  - [Explicit API](#explicit-api)
- [Thread Context Management](#thread-context-management)
- [MDC Integration](#mdc-integration)
- [Modules](#modules)
- [FAQ](#faq)
- [Contributing](#contributing)
- [License](#license)

---

## Features

✅ **Zero-config causality tracing** via Kafka producer interceptor  
✅ **Thread-safe context propagation** using ThreadLocal  
✅ **Automatic MDC lifecycle management** for log correlation  
✅ **No memory leaks** - selective cleanup of ktrace.* keys only  
✅ **Spring Boot auto-configuration** for @KafkaListener  
✅ **Plain Java support** (no framework required)  
✅ **Maven Central ready** under `io.ktrace` group  

---

## Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.ktrace</groupId>
    <artifactId>ktrace-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure Producer

Add the interceptor to your Kafka producer configuration:

```properties
interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor
```

**That's it!** 🎉 Trace context now flows automatically through your Kafka messages.

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Service A (Producer)                        │
│                                                                 │
│  1. Produce message to "orders" topic                          │
│     → Interceptor generates: traceId, spanId                   │
│     → Sets MDC: ktrace.traceId, ktrace.spanId                  │
│     → Injects headers: ktrace-trace-id, ktrace-span-id         │
│     → Publishes TraceEvent to __ktrace topic                   │
│     → Cleanup: MDC.clear() in onAcknowledgement                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Kafka message with headers
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Service B (Consumer)                        │
│                                                                 │
│  2. Consume message from "orders" topic                        │
│     → TraceContextPropagator.extractAndBind(record)            │
│     → Reads headers: ktrace-trace-id, ktrace-span-id           │
│     → Sets ThreadLocal: TraceContext.setCurrent()              │
│     → Sets MDC: ktrace.traceId, ktrace.spanId                  │
│                                                                 │
│  3. Produce message to "notifications" topic                   │
│     → Interceptor reads TraceContext.current()                 │
│     → Generates new spanId (child)                             │
│     → Sets parentSpanId = consumer's spanId (link!)            │
│     → Preserves traceId (chain continuity)                     │
│     → Injects headers with parent-child link                   │
│     → Publishes TraceEvent to __ktrace topic                   │
│     → Cleanup: MDC.clear() + TraceContext.clearCurrent()       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Kafka message with parent link
                              ▼
                    Service C, D, E... (causal chain continues)
```

**Result:** Full causality chain `A → B → C → D` captured in `__ktrace` topic!

---

## Installation

### Maven

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.ktrace</groupId>
            <artifactId>ktrace-bom</artifactId>
            <version>0.1.0-SNAPSHOT</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Plain Java -->
    <dependency>
        <groupId>io.ktrace</groupId>
        <artifactId>ktrace-core</artifactId>
    </dependency>

    <!-- Spring Boot (optional) -->
    <dependency>
        <groupId>io.ktrace</groupId>
        <artifactId>ktrace-spring</artifactId>
    </dependency>
</dependencies>
```

### Gradle

```groovy
implementation platform('io.ktrace:ktrace-bom:0.1.0-SNAPSHOT')
implementation 'io.ktrace:ktrace-core'

// Spring Boot (optional)
implementation 'io.ktrace:ktrace-spring'
```

---

## Usage

### Zero-Config (Interceptor)

The simplest way to enable ktrace is via the Kafka producer interceptor:

```java
// Producer configuration
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// Add ktrace interceptor
props.put("interceptor.classes", "io.ktrace.core.interceptor.KTraceProducerInterceptor");

// Optional: set application name
props.put("ktrace.application-name", "order-service");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

**That's it!** Every `producer.send()` call now:
- ✅ Generates trace context (traceId, spanId) or inherits from consumer
- ✅ Injects ktrace-* headers into the message (zero-config!)
- ✅ Publishes TraceEvent to `__ktrace` topic
- ✅ **No MDC operations in producer** (clean separation of concerns)

**Consumer side (with auto-cleanup):**

```java
consumer.poll(Duration.ofSeconds(1)).forEach(record ->
    KTraceHelper.executeWithTrace(record, r -> {
        // Trace context automatically bound to ThreadLocal + MDC
        log.info("Processing order: {}", r.value());  // [ktrace.traceId=...] in logs
        
        // When producing downstream messages, trace context flows automatically
        producer.send(new ProducerRecord<>("notifications", "key", "value"));
        
        return null;
    })
    // Automatic cleanup: ThreadLocal + MDC cleared, even on exception!
);
```

**What KTraceHelper does automatically:**
1. Extracts trace context from message headers (or creates root if missing)
2. Sets ThreadLocal (for producer to read)
3. Sets MDC (if logging enabled in config)
4. Executes your handler code
5. Cleans up ThreadLocal + MDC (guaranteed, even on exception)

---

### Spring Boot

With `ktrace-spring`, MDC lifecycle is fully automatic:

```xml
<dependency>
    <groupId>io.ktrace</groupId>
    <artifactId>ktrace-spring</artifactId>
</dependency>
```

**application.yml:**

```yaml
spring:
  kafka:
    producer:
      properties:
        interceptor.classes: io.ktrace.core.interceptor.KTraceProducerInterceptor

ktrace:
  enabled: true
  application-name: order-service
```

**Consumer code (zero cleanup needed):**

```java
@KafkaListener(topics = "orders")
public void listen(ConsumerRecord<String, String> record) {
    // ktrace.* keys are automatically in MDC
    log.info("Processing order: {}", record.key());
    
    // When producing, trace context flows automatically
    kafkaTemplate.send("notifications", "key", "value");
    
    // MDC automatically cleaned up after method returns
}
```

---

### Explicit API (Advanced)

For custom handling or advanced use cases:

```java
// Option 1: Extract from headers and bind manually
TraceContext context = TraceContextPropagator.extract(record.headers());
if (context != null) {
    TraceContext.setCurrent(context);
    KTraceMDC.put(context);
}

// Your business logic
processMessage(record);

// Cleanup (don't forget!)
KTraceMDC.clear();
TraceContext.clearCurrent();
```

**Or use the one-shot convenience method:**

```java
try {
    // Extract or create root, set ThreadLocal + MDC
    TraceContextPropagator.extractOrCreateAndBind(record);
    
    // Your business logic
    processMessage(record);
    
} finally {
    // Clean up
    KTraceMDC.clear();
    TraceContext.clearCurrent();
}
```

**Or use the scoped helper (exception-safe):**

```java
KTraceMDC.scoped(context, () -> {
    log.info("Inside scoped block");  // ktrace.* keys present (if enabled)
    processMessage(record);
});
// MDC automatically cleared, even on exception
```

---

## Thread Context Management

### How Context Flows

1. **Consumer receives message** → extracts `ktrace-*` headers → stores in ThreadLocal
2. **Application logic runs** → trace context available via `TraceContext.current()`
3. **Producer sends message** → interceptor reads ThreadLocal → generates child span
4. **After acknowledgement** → cleanup (MDC + ThreadLocal)

### Thread Safety

- **ThreadLocal isolation**: Each thread has its own trace context
- **No cross-thread leaks**: Thread-A's context ≠ Thread-B's context
- **Explicit hand-off** for async code:

```java
TraceContext ctx = KTraceMDC.current();  // Thread-A

executor.submit(() -> {
    KTraceMDC.put(ctx);  // Thread-B
    // ... work with trace context
    KTraceMDC.clear();
});
```

### Memory Leak Prevention

ktrace guarantees **no ThreadLocal memory leaks**:

✅ **Selective clear**: `KTraceMDC.clear()` removes ONLY `ktrace.*` keys  
✅ **Preserves user keys**: `requestId`, `userId`, `sessionId` remain intact  
✅ **Exception safety**: `scoped()` uses try-finally  
✅ **Test verification**: All tests include `@AfterEach` leak detection  

```java
// Before clear:
MDC: {ktrace.traceId=..., ktrace.spanId=..., requestId=req-123, userId=user-456}

// After KTraceMDC.clear():
MDC: {requestId=req-123, userId=user-456}  // ← ktrace.* removed, others preserved
```

---

## MDC Integration

### Configurable Log Correlation

Trace context is **automatically added to logs when logging is enabled**. Users can configure this:

```properties
# Enable trace IDs in logs (default: true)
ktrace.enable-logging=true
```

**When enabled, application logs automatically include trace IDs:**
```
2026-06-19 19:05:00.123 INFO [ktrace.traceId=550e8400-e29b-41d4-a716-446655440000, ktrace.spanId=6ba7b810-9dad-11d1-80b4-00c04fd430c8] Processing order order-123
```

**When disabled, logs are clean (no trace IDs):**
```
2026-06-19 19:05:00.123 INFO Processing order order-123
```

**Important:** Trace headers are **ALWAYS** injected into Kafka messages regardless of logging setting!

### MDC Keys

| MDC Key | Value | Set When |
|---------|-------|----------|
| `ktrace.traceId` | UUID v4 | Logging enabled + consumer context bound |
| `ktrace.spanId` | UUID v4 | Logging enabled + consumer context bound |
| `ktrace.parentSpanId` | UUID v4 | Logging enabled + span has parent |

### Logback Configuration

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%mdc] %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="CONSOLE" />
    </root>
</configuration>
```

### Log4j2 Configuration

```xml
<Configuration>
    <Appenders>
        <Console name="Console">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%X] %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Root level="info">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
```

---

## Modules

### ktrace-core

Plain Java tracing without framework dependencies.

**Dependencies:**
- `kafka-clients` (3.7.0)
- `slf4j-api` (2.0.x)

**Key Classes:**
- `TraceContext` - immutable trace state
- `KTraceMDC` - MDC lifecycle manager
- `TraceContextPropagator` - Kafka header bridge
- `TraceEvent` - trace event model
- `KTraceProducerInterceptor` - zero-config interceptor

### ktrace-spring

Spring Boot auto-configuration.

**Dependencies:**
- `ktrace-core`
- `spring-kafka`
- `spring-boot-autoconfigure`

**Features:**
- Auto-wires producer interceptor
- Auto-cleans MDC for `@KafkaListener`
- Configuration via `application.yml`

### ktrace-tracer

Causal chain reconstruction (coming soon).

**Features:**
- Consumes `__ktrace` topic
- Builds DAG of trace events
- Query API for chain traversal

### ktrace-test

JUnit 5 test utilities (coming soon).

**Features:**
- Embedded Kafka
- TraceEvent assertions
- MDC leak verification

---

## FAQ

### Q: Does ktrace impact performance?

**A:** Minimal impact. The interceptor:
- Generates UUIDs (~1μs)
- Sets 2-3 MDC keys (~100ns each)
- Enqueues TraceEvent to async publisher (non-blocking)
- No I/O in the produce path

### Q: What if the `__ktrace` topic is full?

**A:** The async publisher has a bounded queue. If full, events are dropped with a warning log. **Your application never blocks.**

### Q: Can I use ktrace with reactive Kafka (reactor-kafka)?

**A:** Yes, but you need explicit context hand-off:

```java
Flux<ReceiverRecord<K, V>> messages = receiver.receive();

messages
    .flatMap(record -> {
        TraceContext ctx = TraceContextPropagator.extract(record.headers());
        return Mono.fromCallable(() -> processRecord(record))
            .contextWrite(Context.of("ktrace", ctx));
    })
    .subscribe();
```

### Q: Does ktrace work with Kafka Streams?

**A:** Not yet. Kafka Streams has a different threading model. We're exploring integration options.

### Q: How do I disable ktrace for testing?

**A:** Remove the `interceptor.classes` property, or set:

```properties
ktrace.enabled=false
```

### Q: What about W3C Trace Context or OpenTelemetry?

**A:** ktrace uses `ktrace-*` headers to avoid collisions. If you're using OpenTelemetry for distributed tracing, ktrace complements it by capturing **Kafka-specific causality** (which topic/partition/offset triggered this produce).

---

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

### Development Setup

```bash
# Clone repository
git clone https://github.com/yourusername/ktrace.git
cd ktrace

# Build
mvn clean install

# Run tests
mvn test

# Run specific module tests
mvn test -pl ktrace-core
```

### Running Examples

```bash
# Start Kafka via Docker Compose
cd examples
docker-compose up -d

# Run vanilla Java example
cd vanilla-kafka
mvn exec:java -Dexec.mainClass="io.ktrace.examples.vanilla.VanillaProducerApp"

# Run Spring Boot example
cd ../spring-kafka
mvn spring-boot:run
```

---

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

---

## Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/ktrace/issues)
- **Documentation**: [Wiki](https://github.com/yourusername/ktrace/wiki)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/ktrace/discussions)

---

## Acknowledgments

Inspired by:
- [OpenTelemetry](https://opentelemetry.io/)
- [Spring Cloud Sleuth](https://spring.io/projects/spring-cloud-sleuth)
- [Zipkin](https://zipkin.io/)

Built with ❤️ for the Kafka community.
