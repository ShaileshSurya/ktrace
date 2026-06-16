# Spec: Causal Chain Reconstruction

**Version**: 1  
**Status**: Draft  
**Owner**: ktrace-tracer

## Purpose

Define how the `ktrace-tracer` component consumes `TraceEvent` objects from the `__ktrace` topic and reconstructs the causal DAG (Directed Acyclic Graph) of events across distributed services. This enables "what triggered what" analysis and root-cause tracing.

---

## Data Structures

### CausalNode
Wraps a `TraceEvent` and holds references to parent/children:
```java
class CausalNode {
    TraceEvent event;
    CausalNode parent;      // nullable, null for root
    List<CausalNode> children;
}
```

### CausalChain
A DAG rooted at an origin event (root span):
```java
class CausalChain {
    String traceId;
    CausalNode root;
    Map<String, CausalNode> nodes;  // spanId -> node
}
```

### CausalChainBuilder
Stateful assembler:
```java
class CausalChainBuilder {
    Map<String, CausalNode> allNodes;  // spanId -> node
    
    void ingest(TraceEvent event);
    List<CausalChain> build();
}
```

---

## Scenario 1: Simple Chain (A → B)

**Given** two `TraceEvent` objects published to `__ktrace`:

Event 1 (root):
```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "spanId": "span-A",
  "parentSpanId": null,
  "producedTopic": "orders",
  "producedPartition": 3,
  "producedOffset": 100
}
```

Event 2 (child):
```json
{
  "traceId": "550e8400-e29b-41d4-a716-446655440000",
  "spanId": "span-B",
  "parentSpanId": "span-A",
  "producedTopic": "notifications",
  "producedPartition": 1,
  "producedOffset": 200
}
```

**When** `CausalChainBuilder.ingest()` is called for both events  
**And** `build()` is called

**Then** it must return a single `CausalChain`:
```
CausalChain {
  traceId: "550e8400-e29b-41d4-a716-446655440000"
  root: CausalNode(span-A) {
    parent: null
    children: [CausalNode(span-B)]
  }
  nodes: {
    "span-A" -> CausalNode(span-A),
    "span-B" -> CausalNode(span-B)
  }
}
```

**And** `span-B.parent == span-A`  
**And** `span-A.children == [span-B]`

---

## Scenario 2: Branching Chain (A → B, A → C)

**Given** three events:

Event 1 (root):
```json
{"traceId": "trace-1", "spanId": "span-A", "parentSpanId": null}
```

Event 2 (child B):
```json
{"traceId": "trace-1", "spanId": "span-B", "parentSpanId": "span-A"}
```

Event 3 (child C):
```json
{"traceId": "trace-1", "spanId": "span-C", "parentSpanId": "span-A"}
```

**When** all three are ingested and built

**Then** the chain must have:
```
root: span-A
  ├── span-B
  └── span-C
```

**And** `span-A.children == [span-B, span-C]` (order not guaranteed)  
**And** `span-B.parent == span-A`  
**And** `span-C.parent == span-A`

---

## Scenario 3: Deep Chain (A → B → C → D)

**Given** four events forming a linear chain:
```
span-A (root) → span-B → span-C → span-D
```

**When** ingested and built

**Then** the chain must be:
```
root: span-A
  └── span-B
      └── span-C
          └── span-D
```

**And** each node's `parent` pointer is correct  
**And** each node's `children` list contains exactly 1 element (except D, which has 0)

---

## Scenario 4: Out-of-Order Ingestion (Child Before Parent)

**Given** two events ingested in REVERSE order:

Event 1 (child):
```json
{"traceId": "trace-1", "spanId": "span-B", "parentSpanId": "span-A"}
```

Event 2 (root):
```json
{"traceId": "trace-1", "spanId": "span-A", "parentSpanId": null}
```

**When** `ingest(span-B)` is called first (parent not yet seen)  
**And** `ingest(span-A)` is called second

**Then** the builder must:
1. When ingesting `span-B`: create `CausalNode(span-B)`, store it, note that `span-A` is missing
2. When ingesting `span-A`: create `CausalNode(span-A)`, **link** `span-B.parent = span-A`, add `span-B` to `span-A.children`
3. Final chain: `span-A → span-B` (correctly linked)

**And** the order of ingestion must NOT affect the final DAG structure  
**And** `build()` must handle orphaned nodes (see Scenario 6)

---

## Scenario 5: Patch Event (Update Partition/Offset)

**Given** two events with the SAME `spanId`:

Event 1 (initial):
```json
{
  "traceId": "trace-1",
  "spanId": "span-A",
  "producedPartition": -1,
  "producedOffset": -1
}
```

Event 2 (patch):
```json
{
  "traceId": "trace-1",
  "spanId": "span-A",
  "producedPartition": 3,
  "producedOffset": 500
}
```

**When** both are ingested

**Then** the builder must:
1. Create `CausalNode(span-A)` on first event
2. On second event (same `spanId`), **update** the existing node's event:
   - `producedPartition = 3`
   - `producedOffset = 500`
3. NOT create a duplicate node for the same `spanId`

**And** the final chain has exactly 1 node for `span-A`  
**And** the node's `producedPartition` and `producedOffset` are resolved (not -1)

---

## Scenario 6: Orphan Node (Missing Parent)

**Given** a `TraceEvent` with `parentSpanId="span-X"` but `span-X` never arrives (lost or filtered)

**When** `build()` is called

**Then** the builder must:
1. Return a `CausalChain` with the orphan node as a **pseudo-root**
2. Mark the chain as "incomplete" (future enhancement: `isComplete()` flag)
3. Log a WARNING: "Orphan span detected: span-Y references missing parent span-X"

**And** the orphan node must still be navigable (not dropped)  
**And** this handles late-arriving data or filtered events

---

## Scenario 7: Multiple Traces (traceId Isolation)

**Given** events from TWO different traces:

Trace 1:
```json
{"traceId": "trace-1", "spanId": "span-A", "parentSpanId": null}
{"traceId": "trace-1", "spanId": "span-B", "parentSpanId": "span-A"}
```

Trace 2:
```json
{"traceId": "trace-2", "spanId": "span-X", "parentSpanId": null}
{"traceId": "trace-2", "spanId": "span-Y", "parentSpanId": "span-X"}
```

**When** all four events are ingested  
**And** `build()` is called

**Then** it must return TWO `CausalChain` objects:
```
CausalChain 1: trace-1
  root: span-A → span-B

CausalChain 2: trace-2
  root: span-X → span-Y
```

**And** spans from `trace-1` must NOT appear in `trace-2`'s chain  
**And** the builder must use `traceId` to isolate chains

---

## Scenario 8: TraceReader Consumes and Prints

**Given** a `TraceReader` subscribed to `__ktrace`  
**And** the topic contains events for a chain `A → B → C`

**When** `TraceReader.readAndBuild()` is called

**Then** it must:
1. Poll `KafkaConsumer` for `ConsumerRecords`
2. Deserialize each record to `TraceEvent`
3. Pass each event to `CausalChainBuilder.ingest()`
4. Call `builder.build()` to get assembled chains
5. Call `printChain(chain)` to output:
```
Trace: 550e8400-e29b-41d4-a716-446655440000
  span-A (orders[3]:100)
    └── span-B (notifications[1]:200)
        └── span-C (shipping[0]:300)
```

**And** the output format must be human-readable (tree structure)

---

## Scenario 9: Persistence (Future: TraceStore)

**Given** a `TraceStore` interface:
```java
interface TraceStore {
    void save(CausalChain chain);
    CausalChain load(String traceId);
}
```

**When** chains are built

**Then** they must be persisted to the store (in-memory default, Redis/DB pluggable)  
**And** later queries can retrieve chains by `traceId`

---

## Scenario 10: Cycle Detection (Invalid DAG)

**Given** malformed events that create a cycle:
```
span-A → span-B → span-C → span-A (cycle!)
```

**When** `ingest()` attempts to link `span-C → span-A`

**Then** the builder must:
1. Detect the cycle (A is already an ancestor of C)
2. Log an ERROR: "Cycle detected: span-C → span-A forms a loop"
3. Break the cycle (do NOT add the edge)
4. Mark the chain as "corrupted"

**And** this handles buggy instrumentation or data corruption

---

## Acceptance Criteria

- [ ] `CausalChainBuilder.ingest()` accepts `TraceEvent` objects in any order
- [ ] `build()` returns a list of `CausalChain` objects (one per `traceId`)
- [ ] Parent-child links are established via `parentSpanId → spanId` matching
- [ ] Patch events (same `spanId`) update existing nodes, not create duplicates
- [ ] Out-of-order events (child before parent) are handled correctly
- [ ] Orphan nodes (missing parent) are returned as pseudo-roots with warnings
- [ ] Multiple traces are isolated by `traceId`
- [ ] Cycles are detected and broken
- [ ] `TraceReader` polls `__ktrace`, deserializes, ingests, and prints chains
- [ ] Human-readable tree output shows topic, partition, offset for each span

---

## Test Examples

### Java Test (JUnit 5)

```java
@Test
void simpleChain_shouldLinkAtoB() {
    CausalChainBuilder builder = new CausalChainBuilder();
    
    TraceEvent eventA = TraceEvent.builder()
        .traceId("trace-1").spanId("span-A").parentSpanId(null)
        .build();
    TraceEvent eventB = TraceEvent.builder()
        .traceId("trace-1").spanId("span-B").parentSpanId("span-A")
        .build();
    
    builder.ingest(eventA);
    builder.ingest(eventB);
    
    List<CausalChain> chains = builder.build();
    
    assertThat(chains).hasSize(1);
    CausalChain chain = chains.get(0);
    assertThat(chain.getRoot().getEvent().getSpanId()).isEqualTo("span-A");
    assertThat(chain.getRoot().getChildren()).hasSize(1);
    assertThat(chain.getRoot().getChildren().get(0).getEvent().getSpanId()).isEqualTo("span-B");
}

@Test
void outOfOrder_childBeforeParent_shouldLink() {
    CausalChainBuilder builder = new CausalChainBuilder();
    
    TraceEvent eventB = TraceEvent.builder()
        .traceId("trace-1").spanId("span-B").parentSpanId("span-A")
        .build();
    TraceEvent eventA = TraceEvent.builder()
        .traceId("trace-1").spanId("span-A").parentSpanId(null)
        .build();
    
    builder.ingest(eventB);  // child first
    builder.ingest(eventA);  // parent second
    
    List<CausalChain> chains = builder.build();
    
    assertThat(chains).hasSize(1);
    assertThat(chains.get(0).getRoot().getEvent().getSpanId()).isEqualTo("span-A");
    assertThat(chains.get(0).getNodes().get("span-B").getParent().getEvent().getSpanId()).isEqualTo("span-A");
}

@Test
void patchEvent_shouldUpdateExistingNode() {
    CausalChainBuilder builder = new CausalChainBuilder();
    
    TraceEvent initial = TraceEvent.builder()
        .traceId("trace-1").spanId("span-A").producedPartition(-1).producedOffset(-1)
        .build();
    TraceEvent patch = TraceEvent.builder()
        .traceId("trace-1").spanId("span-A").producedPartition(3).producedOffset(500)
        .build();
    
    builder.ingest(initial);
    builder.ingest(patch);
    
    List<CausalChain> chains = builder.build();
    
    assertThat(chains).hasSize(1);
    assertThat(chains.get(0).getNodes()).hasSize(1);  // not 2
    assertThat(chains.get(0).getNodes().get("span-A").getEvent().getProducedPartition()).isEqualTo(3);
}

@Test
void multipleTraces_shouldIsolate() {
    CausalChainBuilder builder = new CausalChainBuilder();
    
    builder.ingest(TraceEvent.builder().traceId("trace-1").spanId("span-A").parentSpanId(null).build());
    builder.ingest(TraceEvent.builder().traceId("trace-1").spanId("span-B").parentSpanId("span-A").build());
    builder.ingest(TraceEvent.builder().traceId("trace-2").spanId("span-X").parentSpanId(null).build());
    builder.ingest(TraceEvent.builder().traceId("trace-2").spanId("span-Y").parentSpanId("span-X").build());
    
    List<CausalChain> chains = builder.build();
    
    assertThat(chains).hasSize(2);
    assertThat(chains).extracting(CausalChain::getTraceId).containsExactlyInAnyOrder("trace-1", "trace-2");
}
```

---

## Related Specs

- [TraceEvent Schema](trace-event-schema.spec.md) — the input to chain reconstruction
- [Kafka Header Contract](kafka-headers.spec.md) — how parent-child links are propagated
