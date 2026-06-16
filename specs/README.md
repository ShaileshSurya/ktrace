# ktrace Specifications

This directory contains **executable specifications** for ktrace's core contracts. These specs drive development via **Spec-Driven Development (SDD)** — write the spec first, then implement code that satisfies it.

---

## Specification Files

| Spec | Owns | What It Defines |
|------|------|-----------------|
| [trace-event-schema.spec.md](trace-event-schema.spec.md) | `ktrace-core` | Canonical `TraceEvent` JSON schema, serialization, field validation, versioning |
| [kafka-headers.spec.md](kafka-headers.spec.md) | `ktrace-core` | Exact header names (`ktrace-*` prefix), injection/extraction, collision avoidance |
| [mdc-lifecycle.spec.md](mdc-lifecycle.spec.md) | `ktrace-core` | SLF4J MDC keys, set/clear lifecycle, leak prevention, ThreadLocal isolation |
| [interceptor-behavior.spec.md](interceptor-behavior.spec.md) | `ktrace-core` | `KTraceProducerInterceptor` end-to-end: context extraction, header injection, event publish, MDC |
| [causal-chain.spec.md](causal-chain.spec.md) | `ktrace-tracer` | DAG reconstruction from `TraceEvent` objects, parent-child linking, out-of-order handling |

---

## How to Use These Specs

### 1. Read the Spec Before Coding

Before implementing a component:
1. Read the relevant spec file (e.g., `trace-event-schema.spec.md` before writing `TraceEvent.java`)
2. Understand the **scenarios** (Given/When/Then)
3. Note the **acceptance criteria** (checklist at the end)

The spec is your contract — it defines **what** the code must do, not **how**.

---

### 2. Write Tests from Scenarios

Each spec includes **test examples** at the end. Use these as templates:

**Example**: From `trace-event-schema.spec.md`, Scenario 1:
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
        // ... other fields
        .build();

    assertThat(event.getParentSpanId()).isNull();
    assertThat(event.getTriggerTopic()).isNull();
    assertThat(event.getProducedPartition()).isEqualTo(-1);
}
```

**Workflow**:
1. Copy the test from the spec
2. Run it (it will fail — no implementation yet)
3. Implement the code to make it pass
4. Move to the next scenario

This is **Test-Driven Development (TDD)**, driven by the spec.

---

### 3. Validate Against Acceptance Criteria

At the end of each spec is an **Acceptance Criteria** checklist. After implementing, verify:

```markdown
- [x] `TraceEvent` class is immutable (all fields `final`, no setters)
- [x] All non-nullable fields throw `NullPointerException` if null passed to constructor
- [x] `traceId` and `spanId` are validated as UUID v4 format
- [ ] `producedPartition` and `producedOffset` default to -1 when unknown  ← NOT DONE
```

This checklist is your **definition of done**.

---

### 4. Update the Spec When Design Changes

If you discover a better approach during implementation:
1. **Update the spec first** (add a new scenario or modify existing)
2. Update the tests
3. Update the code

The spec is the source of truth. Code changes without spec updates = drift.

---

## Spec Format (Gherkin-Style)

Each scenario follows **Given/When/Then** structure:

```markdown
## Scenario N: Descriptive Title

**Given** preconditions (initial state, context)
**And** additional preconditions

**When** action or event occurs

**Then** expected outcome
**And** additional expectations
**And** constraints or invariants
```

This format is:
- **Human-readable**: non-technical stakeholders can review
- **Executable**: maps directly to test code
- **Precise**: no ambiguity about expected behavior

---

## Integration with Speckit (Optional)

These `.spec.md` files can be executed via **Speckit** (or similar BDD frameworks):

### Option A: Manual Validation
Read the spec, write JUnit tests, manually verify all scenarios pass.

### Option B: Speckit Runner (Automated)
```java
@RunWith(SpeckitRunner.class)
@SpecFile("specs/trace-event-schema.spec.md")
public class TraceEventSchemaSpecTest {
    // Speckit auto-generates test methods from scenarios
}
```

For now, **Option A (manual)** is recommended until you're familiar with the specs.

---

## SDD Workflow Summary

```
1. Read spec (understand scenarios + acceptance criteria)
   ↓
2. Write failing test (from spec example)
   ↓
3. Implement minimal code to pass test
   ↓
4. Refactor (keep tests green)
   ↓
5. Move to next scenario
   ↓
6. All scenarios pass? Check acceptance criteria
   ↓
7. Done! (Spec = Living Documentation)
```

---

## Why Spec-Driven Development?

| Benefit | Explanation |
|---------|-------------|
| **Clear contracts** | No ambiguity about what code should do |
| **Regression safety** | Specs catch breaking changes when adding features |
| **Living documentation** | Specs stay up-to-date (code enforces them) |
| **Team alignment** | Non-coders can read Given/When/Then scenarios |
| **TDD with guidance** | Specs provide the test cases, not invented ad-hoc |

---

## Related Files

- [TASKS.md](../TASKS.md) — implementation task breakdown (references these specs)
- [PLAN.md](../PLAN.md) — high-level architecture and module layout
- [CLAUDE.md](../CLAUDE.md) — minimal project context for AI agents

---

## Next Steps

1. Start with `trace-event-schema.spec.md` — it's the foundation
2. Implement `TraceEvent.java` + serializer to satisfy all scenarios
3. Move to `kafka-headers.spec.md` — implement `TraceContextPropagator`
4. Continue through specs in order (they build on each other)

Happy spec-driven coding! 🚀
