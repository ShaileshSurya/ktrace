# ktrace

Minimalist Java SDK for Kafka causality tracing. Intercepts producer calls, publishes trace events to `__ktrace` topic.

## Structure

Maven multi-module: `ktrace-core` (plain Java), `ktrace-spring` (Spring Boot 3.x), `ktrace-tracer` (chain reconstruction), `ktrace-test` (JUnit utilities), `examples/` (vanilla + Spring PoC).

## Key Constraints

- Let me know if something needs to be changed in claude.md
- When you make assumption that this might needed, ask. I want you to be an assistant for my coding. 
- We want to move to plug and play model, if something is going beyond it, tell explicitly. Do not assume anything. 
- While doing implementation always ask, for each and every block that you are going to implement, get it reviewed. 
## Reference

See `PLAN.md` for full schema, class layout, and implementation order.
