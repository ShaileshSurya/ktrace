# ktrace Reference

## Project Modules

- **ktrace-core** — Core SDK (TraceEvent, TraceContext, interceptor, serialization)
- **ktrace-spring** — Spring Boot 3.x auto-configuration
- **ktrace-tracer** — Causal chain reconstruction from `__ktrace` topic
- **ktrace-test** — JUnit 5 test utilities (embedded Kafka, assertions)
- **examples/** — Vanilla Java + Spring Boot examples

---

## Kafka Headers (`ktrace-` prefix)

| Header | Type | Description |
|--------|------|-------------|
| `ktrace-trace-id` | UUID v4 | Top-level chain identifier |
| `ktrace-span-id` | UUID v4 | This produce call |
| `ktrace-parent-span-id` | UUID v4 | Triggering span (optional) |

---

## TraceEvent Schema (published to `__ktrace`)

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `traceId` | String (UUID) | No | Top-level chain identifier |
| `spanId` | String (UUID) | No | This produce call |
| `parentSpanId` | String (UUID) | Yes | Triggering span |
| `producedTopic` | String | No | Target topic |
| `producedPartition` | int | No | -1 before ack |
| `producedOffset` | long | No | -1 before ack |
| `triggerTopic` | String | Yes | Trigger topic |
| `triggerPartition` | int | No | -1 if none |
| `triggerOffset` | long | No | -1 if none |
| `triggerConsumerGroup` | String | Yes | Trigger consumer group |
| `producerTimestampMs` | long | No | Intercept timestamp |
| `clientId` | String | No | Kafka client.id |
| `applicationName` | String | Yes | From config |
| `messageKey` | String | Yes | Record key (toString) |
| `messageSizeBytes` | int | No | Serialized value size |
| `schemaVersion` | int | No | Currently 1 |

---

## TraceContext (ThreadLocal Storage)

Stored per-thread to enable zero-config propagation through interceptors.

### Core API (`io.ktrace.core.context.TraceContext`)

```java
// Access ThreadLocal context
static TraceContext current()                    // Get current context
static void setCurrent(TraceContext context)     // Set current context
static void clearCurrent()                       // Clear ThreadLocal (prevent leaks)
static TraceContext currentOrCreateRoot()        // Get or create root context

// Create contexts
TraceContext.root(traceId, spanId)              // Root span (no parent)
TraceContext.child(traceId, spanId, parentSpanId) // Child span

// Trigger metadata (consumer → producer flows)
static void setTrigger(topic, partition, offset, group)
static TriggerMetadata getTrigger()
```

### Propagation API (`io.ktrace.core.context.TraceContextPropagator`)

```java
// Extract/inject from Kafka headers
static TraceContext extract(Headers headers)
static void inject(TraceContext ctx, Headers headers)

// Consumer convenience methods
static void extractAndBind(ConsumerRecord<?, ?> record)      // Extract + ThreadLocal + MDC
static void extractOrCreateAndBind(ConsumerRecord<?, ?> record) // Extract or create root
```

**Producer flow**: Interceptor reads `TraceContext.current()`, generates new span, injects headers  
**Consumer flow**: Call `extractAndBind(record)` to set context from incoming headers
