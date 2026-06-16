# ktrace

Minimalist Java SDK for Kafka causality tracing. Intercepts producer calls, publishes trace events to `__ktrace` topic.

## Structure

Maven multi-module: `ktrace-core` (plain Java), `ktrace-spring` (Spring Boot 3.x), `ktrace-tracer` (chain reconstruction), `ktrace-test` (JUnit utilities), `examples/` (vanilla + Spring PoC).

## Key Constraints

- **Headers**: Use `ktrace-` prefix (avoid `kafka_*`, `traceparent`, `X-B3-*`)
- **MDC**: Set `ktrace.traceId/spanId/parentSpanId` in logs, clear after produce/consume to prevent leaks
- **Non-blocking**: `AsyncTracePublisher` uses bounded queue + daemon thread
- **Maven Central ready**: GPG signing, sources/javadoc, `io.ktrace` group

## Reference

See `PLAN.md` for full schema, class layout, and implementation order.
