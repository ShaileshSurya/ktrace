# ktrace Implementation Tasks

## Phase 1: Foundation

### Task 1.1: Maven Project Structure ✅ COMPLETED
- [x] Create parent `pom.xml` with:
  - `${revision}` property for version management
  - Plugin management: `maven-compiler-plugin` (Java 11+), `maven-source-plugin`, `maven-javadoc-plugin`, `maven-gpg-plugin`, `central-publishing-maven-plugin`, `maven-flatten-plugin`, `maven-release-plugin`
  - Dependency management: `kafka-clients`, `slf4j-api`, `spring-kafka`, `spring-boot-autoconfigure`, `junit-jupiter`, `assertj-core`
  - Metadata: `<scm>`, `<licenses>` (Apache 2.0), `<developers>`, `<url>`
  - Modules: `ktrace-bom`, `ktrace-core`, `ktrace-spring`, `ktrace-tracer`, `ktrace-test`, `examples`
- [x] Create `ktrace-bom/pom.xml` (Bill of Materials, `<dependencyManagement>` only)
- [x] Create placeholder `pom.xml` for each module
- [x] Verify: `mvn clean compile` succeeds from root ✅ BUILD SUCCESS

### Task 1.2: ktrace-core - Event Model
- [x] `TraceEvent.java` - immutable value object with all schema fields ✅ **SCENARIO 1 COMPLETE**
  - [x] Builder pattern implemented
  - [x] UUID validation for traceId/spanId/parentSpanId
  - [x] Required field validation (NPE on null)
  - [x] Immutability (all fields final, no setters)
  - [x] equals/hashCode/toString
  - [x] 12 unit tests passing (TraceEventTest.java)
- [ ] `TraceEventSerializer.java` - Kafka `Serializer<TraceEvent>` impl (JSON via Jackson or minimal hand-rolled)
- [ ] `TraceEventDeserializer.java` - Kafka `Deserializer<TraceEvent>`
- [ ] Unit tests: serialize/deserialize round-trip, null field handling

### Task 1.3: ktrace-core - Context & MDC
- [ ] `TraceContext.java` - immutable holder for traceId/spanId/parentSpanId, factory methods `root()`, `child(parentSpanId)`
- [ ] `KTraceMDC.java`:
  - `put(TraceContext)` - set `ktrace.traceId`, `ktrace.spanId`, `ktrace.parentSpanId` in `org.slf4j.MDC`
  - `clear()` - remove only `ktrace.*` keys
  - `current()` - read back from MDC
  - `scoped(TraceContext, Runnable)` - try-finally helper
- [ ] `TraceContextPropagator.java`:
  - `inject(TraceContext, Headers)` - write `ktrace-trace-id`, `ktrace-span-id`, `ktrace-parent-span-id` headers
  - `extract(Headers)` - read headers into `TraceContext`
  - `extractAndBind(ConsumerRecord)` - extract + set `TraceContext` ThreadLocal + MDC
- [ ] Unit tests: header round-trip, MDC set/clear, no leak after clear

### Task 1.4: ktrace-core - Config
- [ ] `KTraceConfig.java` - builder pattern with defaults:
  - `traceTopic` (default `"__ktrace"`)
  - `enabled` (default `true`)
  - `asyncQueueSize` (default `1000`)
  - `applicationName` (optional, from env `KTRACE_APP_NAME`)
  - `samplingRate` (default `1.0`, future: 0.0-1.0)
- [ ] Unit tests: builder, defaults, validation

### Task 1.5: ktrace-core - Publisher
- [ ] `TracePublisher.java` - interface `void publish(TraceEvent event)`
- [ ] `AsyncTracePublisher.java`:
  - Holds dedicated `KafkaProducer<String, TraceEvent>` (key = traceId)
  - `LinkedBlockingQueue<TraceEvent>` bounded by `asyncQueueSize`
  - Single daemon thread drains queue, calls `producer.send()` fire-and-forget
  - `close()` - drain queue, close producer
  - Graceful degradation: if queue full, log warning + drop (never block app thread)
- [ ] Unit tests: mock producer, assert events published, queue overflow behavior

### Task 1.6: ktrace-core - Interceptor
- [ ] `KTraceProducerInterceptor.java` - implements `ProducerInterceptor<K,V>`:
  - `configure(Map<String, ?> configs)` - read `KTraceConfig` from producer config (prefix `ktrace.`)
  - `onSend(ProducerRecord<K,V> record)`:
    1. Extract incoming context from record headers via `TraceContextPropagator.extract()`
    2. Generate new `spanId` (UUID), inherit or create `traceId`
    3. Call `KTraceMDC.put(context)`
    4. Inject `ktrace-*` headers into record
    5. Build `TraceEvent` (partition/offset = -1)
    6. Enqueue to `AsyncTracePublisher`
    7. Return modified record
  - `onAcknowledgement(RecordMetadata metadata, Exception exception)`:
    1. Publish "patch" event with resolved partition/offset
    2. Call `KTraceMDC.clear()`
  - `close()` - close publisher
- [ ] Handle trigger context: if `TraceContext.current()` has trigger metadata, include in `TraceEvent`
- [ ] Unit tests: mock publisher, assert context propagation, MDC lifecycle

### Task 1.7: ktrace-core - Explicit Wrapper
- [ ] `KTraceProducer.java` - wraps `KafkaProducer<K,V>`:
  - `static <K,V> KTraceProducer<K,V> wrap(KafkaProducer<K,V> delegate, KTraceConfig config)`
  - Delegates all methods to underlying producer
  - `send(ProducerRecord<K,V>)` - same logic as interceptor `onSend()`/`onAcknowledgement()`, but wraps the callback
  - `withContext(TraceContext ctx)` - explicit context injection (for reactive)
- [ ] Unit tests: assert delegation, context propagation, MDC lifecycle

## Phase 2: Testing Infrastructure

### Task 2.1: ktrace-test - Embedded Kafka
- [ ] `KTraceEmbeddedKafka.java` - JUnit 5 extension:
  - Starts embedded Kafka broker (use `kafka-clients` test-scope or lightweight embedded lib)
  - Pre-creates `__ktrace` topic
  - Exposes `bootstrapServers()` for test config
  - `@AfterEach` - reset state
- [ ] `TraceEventAssert.java` - AssertJ-style fluent assertions:
  - `assertThat(event).hasTraceId(expected).hasParentSpanId(expected).hasProducedTopic(expected)`
- [ ] Integration test: produce record, consume from `__ktrace`, assert event received within 2s

## Phase 3: Examples - Vanilla Kafka

### Task 3.1: Example Infrastructure
- [ ] `examples/pom.xml` - parent for example modules
- [ ] `examples/docker-compose.yml`:
  - Kafka broker (single node, `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS`)
  - Zookeeper
  - Expose `localhost:9092`
- [ ] `examples/vanilla-kafka/pom.xml` - depends on `ktrace-core`, `logback-classic`

### Task 3.2: Vanilla Producer
- [ ] `VanillaProducerApp.java`:
  - Create `KafkaProducer` with `interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor`
  - Send 3 records to `orders` topic (key = orderId, value = JSON order)
  - Log before/after each send with `logger.info("Sending order {}", orderId)` (verify `ktrace.traceId` appears)
  - Sleep 1s, close producer
- [ ] `logback.xml` - pattern includes `%X{ktrace.traceId}` MDC key

### Task 3.3: Vanilla Consumer
- [ ] `VanillaConsumerApp.java`:
  - Subscribe to `orders`
  - For each record:
    1. Call `TraceContextPropagator.extractAndBind(record)` (sets MDC)
    2. Log `logger.info("Processing order {}", record.key())`
    3. Produce to `notifications` topic (trigger context inherited)
    4. Call `KTraceMDC.clear()` in finally block
  - Consumer also subscribes to `__ktrace` (separate thread) and prints causal chain to stdout

### Task 3.4: Vanilla E2E Test
- [ ] Run `docker-compose up -d`
- [ ] Run `VanillaProducerApp`
- [ ] Run `VanillaConsumerApp`
- [ ] Verify: `__ktrace` contains 6 events (3 from producer, 3 from consumer chained)
- [ ] Grep logs for `ktrace.traceId`, assert present during produce/consume, absent after clear

## Phase 4: Spring Boot Integration

### Task 4.1: ktrace-spring - Auto-Configuration
- [ ] `KTraceProperties.java` - `@ConfigurationProperties(prefix = "ktrace")`:
  - `enabled` (default true)
  - `traceTopic`
  - `asyncQueueSize`
  - `applicationName`
- [ ] `KTraceAutoConfiguration.java`:
  - `@ConditionalOnProperty(name = "ktrace.enabled", matchIfMissing = true)`
  - Create `KTraceConfig` bean from `KTraceProperties`
  - Create `AsyncTracePublisher` bean
  - Register `KTraceProducerFactoryCustomizer`
  - Register `KTraceMDCKafkaListenerInterceptor` via `ConcurrentKafkaListenerContainerFactoryConfigurer` or `ContainerCustomizer`
- [ ] `src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` - list `KTraceAutoConfiguration`

### Task 4.2: ktrace-spring - Producer Customizer
- [ ] `KTraceProducerFactoryCustomizer.java` - implements `ProducerFactoryCustomizer`:
  - Read existing `interceptor.classes` from producer config
  - Append `io.ktrace.core.interceptor.KTraceProducerInterceptor` (comma-separated)
  - Inject `ktrace.*` config properties for interceptor to read

### Task 4.3: ktrace-spring - Consumer Interceptor
- [ ] `KTraceMDCKafkaListenerInterceptor.java` - implements `RecordInterceptor`:
  - `intercept(ConsumerRecord<K,V> record)`:
    1. Call `TraceContextPropagator.extractAndBind(record)`
    2. Return record
  - `success(ConsumerRecord<K,V> record, Object result)` - call `KTraceMDC.clear()`
  - `failure(ConsumerRecord<K,V> record, Exception exception)` - call `KTraceMDC.clear()`
- [ ] Wire into `@KafkaListener` containers via auto-config

### Task 4.4: ktrace-spring - Annotation (Optional)
- [ ] `@EnableKafkaTracing` - `@Import(KTraceAutoConfiguration.class)` for non-Boot users

### Task 4.5: Spring Boot Test
- [ ] Spring Boot test with `@EmbeddedKafka`:
  - Assert `KTraceProducerInterceptor` bean exists
  - Send via `KafkaTemplate`, consume with `@KafkaListener`
  - Assert `TraceEvent` published to `__ktrace`
  - Assert MDC set/cleared

## Phase 5: Examples - Spring Kafka

### Task 5.1: Spring Producer
- [ ] `examples/spring-kafka/pom.xml` - Spring Boot starter, `ktrace-spring`
- [ ] `SpringProducerApp.java`:
  - `@SpringBootApplication`
  - Autowire `KafkaTemplate`
  - Send 3 records to `orders` topic via `template.send()`
  - Log with MDC before/after
- [ ] `application.properties`:
  - `ktrace.enabled=true`
  - `ktrace.application-name=spring-producer`

### Task 5.2: Spring Consumer
- [ ] `SpringConsumerApp.java`:
  - `@KafkaListener(topics = "orders")`
  - Log received record (assert `ktrace.traceId` in MDC)
  - Produce to `notifications` via `KafkaTemplate`
  - No manual MDC management (auto via interceptor)

### Task 5.3: Spring E2E Test
- [ ] Run both apps, verify `__ktrace` contains linked events
- [ ] Verify auto-configuration wired interceptor (no user config beyond `ktrace.enabled`)
- [ ] Grep logs for MDC keys

## Phase 6: Tracer Component

### Task 6.1: ktrace-tracer - Model
- [ ] `CausalNode.java` - wraps `TraceEvent`, holds references to parent/children nodes
- [ ] `CausalChain.java` - DAG of `CausalNode`s, rooted at origin event
- [ ] `TraceStore.java` - interface for persistence (in-memory default impl)

### Task 6.2: ktrace-tracer - Builder
- [ ] `CausalChainBuilder.java`:
  - Stateful: maintains map `spanId -> CausalNode`
  - `ingest(TraceEvent event)` - creates node, links to parent via `parentSpanId`
  - `build()` - returns list of `CausalChain`s (one per root traceId)
- [ ] Handle "patch" events (same spanId, updated partition/offset)

### Task 6.3: ktrace-tracer - Reader
- [ ] `TraceReader.java`:
  - `KafkaConsumer` subscribed to `__ktrace`
  - Polls, deserializes `TraceEvent`, passes to `CausalChainBuilder`
  - `printChains()` - pretty-print DAG to stdout

### Task 6.4: Tracer Integration Test
- [ ] Produce 2 services' worth of events (A → B → C chain)
- [ ] Run `TraceReader`, assert chain reconstructed correctly

## Phase 7: Polish & Publishing

### Task 7.1: Documentation
- [ ] `README.md` - quickstart, Maven coordinates, usage examples
- [ ] Javadoc for public API classes
- [ ] `CONTRIBUTING.md` - build instructions, release process

### Task 7.2: Header Collision Verification
- [ ] Grep test logs/assertions to confirm no `kafka_*`, `traceparent`, `X-B3-*` used by ktrace

### Task 7.3: MDC Leak Tests
- [ ] Add `@AfterEach` to all tests: `assertNull(MDC.get("ktrace.traceId"))`

### Task 7.4: Maven Central Preparation
- [ ] Set up Sonatype OSSRH account, claim `io.ktrace` group
- [ ] Configure GPG signing keys
- [ ] Test `mvn deploy` to staging repo
- [ ] `mvn release:prepare && mvn release:perform`

---

## Quick Start Commands

```bash
# Build all modules
mvn clean install

# Run vanilla example
cd examples/vanilla-kafka
docker-compose up -d
mvn exec:java -Dexec.mainClass="io.ktrace.examples.vanilla.VanillaProducerApp"
mvn exec:java -Dexec.mainClass="io.ktrace.examples.vanilla.VanillaConsumerApp"

# Run Spring example
cd examples/spring-kafka
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=producer"
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=consumer"
```
