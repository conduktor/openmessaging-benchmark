# CHOP `THROUGHPUT` verdict — design

- **Date:** 2026-07-24
- **Status:** Design approved; implementation not started
- **Component:** `benchmark-framework` — `RampRateFinder` (opt-in CHOP rate finder), PR #19
- **Related:** `benchmark-framework/RATE_FINDING.md`; the two "Trial finding" incidents recorded there

## Context

CHOP discovers a sustainable producer rate by holding a candidate rate and checking whether it
"held." Today that check is **backlog-count based**: a candidate fails if `receiveBacklog` or
`publishBacklog` exceeds a message-count `limit` (absolute for AIMD, or `rate × rampMaxBacklogSeconds`
clamped to `[rampMaxBacklogFloor, rampMaxBacklogCeiling]` for CHOP).

An AKS re-validation on 2026-07-24 (matrix `finder-chop-revalidate.yaml`, digest-pinned image
`sha256:4c43c65…`, `representative` preset, 1 topic / 100 partitions / 100-byte messages) exposed
that this predicate is structurally wrong. Three loads on the **same cluster**:

|     Load     |            Config            |                Discovered rate                |                                                Sustained over 15 min?                                                |
|--------------|------------------------------|-----------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| AIMD         | fixed 1000-msg backlog limit | ~13,500 msg/s                                 | yes (but conservative)                                                                                               |
| chop-floor   | `rampMaxBacklogSeconds: 0.1` | **2,734 msg/s** (clean confirm)               | yes, trivially — far under capacity                                                                                  |
| chop-ceiling | `rampMaxBacklogSeconds: 1.0` | **880,000 msg/s** (`nonMonotonic` → withheld) | **yes** — publish≈consume≈880k, backlog bounded (max 16,432, not growing), avg publish-delay 0.12 ms, e2e p99 326 ms |

The true sustainable rate was **~880k msg/s**. AIMD under-reported it by ~65×, and `chop-floor` by
~320×.

**Root cause.** A healthy pipeline's natural in-flight backlog *scales with throughput* (~16k
messages in flight at 880k msg/s; ~1k at 13k msg/s). So no fixed-ish message count is correct at
once: a count strict enough to catch overload at low rates (≈1000) trips on the normal in-flight
backlog of a fast pipeline and under-reports; a count loose enough for high rates is meaningless at
low rates and lets bracket run away. Because the "clean" baseline moves with the rate, the predicate
is also **non-monotone** in rate — which violates binary chop's key prerequisite and is why
`nonMonotonic` fired on the *correct* 880k result (a false alarm that withheld the one good answer).

## Goals

- Make CHOP's verdict depend on **scale-free** signals so one threshold is correct at every rate and
  message size — fixing both the under-report (low rates) and the runaway (high rates) with a single
  mechanism, and restoring monotonicity so chop is valid and `nonMonotonic` stops false-firing.
- Ship it as an **opt-in verdict mode**, leaving the current behavior (and PR #19's semantics) as the
  default — consistent with how CHOP itself was added alongside AIMD.

## Non-goals

- Removing the backlog-count predicate or its knobs (kept for the default `BACKLOG` mode).
- Adding a latency-based gate (achieved-rate + consumer-drain cover both failure modes; a latency
  gate can be a later addition if wanted).
- Changing the bracket / chop / confirmation-hold / reopen-on-failed-confirm / safety-cap /
  `nonMonotonic` machinery — all of it is reused unchanged.
- Fixing hold-length sensitivity (see Risks) — orthogonal to this change.

## Design

### Config surface (`Workload`)

- **`rampVerdict`** — new enum `RampVerdict { BACKLOG, THROUGHPUT }`, default `BACKLOG`. Selects how
  a hold's clean/exceeded verdict is decided. Applies only to CHOP (`producerRate: 0`,
  `rampAlgorithm: CHOP`).
- **`rampMinThroughputRatio`** — new `Double`, default `0.95`. Used only when
  `rampVerdict: THROUGHPUT`. The minimum fraction of target throughput (and of consumer drain) a
  candidate must achieve to count as clean.
- Existing `rampMaxBacklogSeconds`, `rampMaxBacklogFloor`, `rampMaxBacklogCeiling`,
  `rampPublishBacklogLimit`, `rampReceiveBacklogLimit` — unchanged, used only under
  `rampVerdict: BACKLOG`.

### The predicate

Under `THROUGHPUT`, a candidate is judged **at the end of each hold** (bracket hold or chop hold),
over that hold's window `[t0, t1]` at the fixed candidate `rate`:

```
expected  = rate × (t1 − t0)
published = totalPublished(t1) − totalPublished(t0)
received  = totalReceived(t1)  − totalReceived(t0)

producerKeepsUp = published ≥ rampMinThroughputRatio × expected
consumerKeepsUp = received  ≥ rampMinThroughputRatio × published

clean  ⇔  producerKeepsUp AND consumerKeepsUp     (otherwise: exceeded)
```

- Both gates are **fractions**, so the single `rampMinThroughputRatio` is correct at every rate and
  message size — no floor/ceiling/count tuning.
- `consumerKeepsUp` compares to *published*, not *expected*: a lagging consumer is not
  double-penalised for messages the producer never sent (producer shortfall is already caught by
  `producerKeepsUp`). Over a full hold, a consumer that keeps up yields `received ≈ published`.
- Validation against the AKS data: 880k → `published/expected ≈ 1.00`, `received/published ≈ 1.00` →
  **clean** (correct); a 1.8M candidate that only achieved ~1.3–1.5M → `≈ 0.72–0.83` → **exceeded**
  (correctly rejected); low rates where producer and consumer both keep up → clean, so discovery
  **keeps climbing** instead of sticking at 2,734.

### Integration with the existing finder

- The **only** change to the state machine is the function that turns a completed hold into a
  `clean`/`exceeded` boolean. `RampRateFinder` gains a hold-start snapshot of the cumulative
  counters (`totalPublished`, `totalReceived`) so it can compute the two ratios when the hold
  completes; a small strategy branch selects `BACKLOG` (existing per-poll count check) vs
  `THROUGHPUT` (this ratio check).
- Bracket doubling/halving, chop bisection, confirmation holds, reopen-on-failed-confirm, the safety
  cap, and `nonMonotonic` recording are **reused verbatim** — they consume the boolean and are
  agnostic to how it was produced.
- **No new `Worker` API.** The finder already receives cumulative `totalPublished`/`totalReceived`
  each poll, so this is distributed-worker compatible unchanged.

### Behavioral difference: no per-poll fast-fail under `THROUGHPUT`

The ratios need the whole hold window, so a `THROUGHPUT` candidate is judged only at hold
completion — there is no mid-hold fast-fail. (`BACKLOG` keeps its current per-poll fast-fail on a
count breach.) Consequence: discovery is slower under `THROUGHPUT` (every candidate, including
doomed high overshoots during bracket, runs its full hold). This is an accepted trade for a
correct, scale-free verdict.

## Back-compat & migration

- Default `rampVerdict: BACKLOG` ⇒ **zero behavior change**; existing workloads and PR #19 semantics
  are untouched.
- `THROUGHPUT` is purely opt-in via the new field. Existing example workload YAMLs are unaffected.
- `rampVerification` output shape is unchanged. Under `THROUGHPUT`, `nonMonotonic` is expected to be
  `false` far more often (monotone predicate), so the object will be present (and the rate reported)
  on runs that the count predicate would have withheld.

## Testing plan

- **Unit (`RampRateFinderTest`)** — extend the fake broker to also model consumer drain (a
  sustained consume capacity), then add `THROUGHPUT` cases:
  1. climbs past the rate where a 1000-message count limit would stick (the under-report fix);
  2. rejects an oversubscribed rate (`achievedFraction` below the ratio);
  3. fails when the producer keeps up but the consumer lags (`consumerKeepsUp` false);
  4. `nonMonotonic` stays `false` across a clean monotone climb it would previously have tripped.
- **Integration (`ChopRateFinderKafkaIT`, `-Pintegration-tests`)** — add a `THROUGHPUT` variant that
  asserts it converges to a sustainable rate **≥** the `BACKLOG` path on the same broker (relative
  assertion, machine-independent), reusing the existing sustainability check (bounded publish-delay).
- **Docs (`RATE_FINDING.md`)** — document both verdict modes and the predicate; record the AKS
  finding (880k sustained vs 2,734 under-report vs 13.5k AIMD) as the motivation; note the
  hold-length guideline applies to both modes.

## Risks & open questions

- **Hold-length sensitivity is not solved by this change.** A hold shorter than the broker's
  burst-absorption time can still see `published ≈ expected` during a burst and pass an unsustainable
  rate. This is orthogonal (it affects `BACKLOG` too) and is covered by a separate hold-length
  guideline. `THROUGHPUT` fixes the *scale/count* problem, not the *hold-duration* problem.
- **Client-side rate-limiter bottleneck.** If OMB's own rate limiter cannot schedule the target
  rate, `producerKeepsUp` fails for a non-broker reason. That is still the honest benchmark answer
  ("this setup can't sustain this rate"); if disambiguation is ever needed, publish-delay separates
  producer-side stalls from broker saturation.
- **Threshold default (0.95).** A fraction, so one value spans all rates, but the exact number is a
  judgment call; 0.95 gives ~5% slack for jitter over a 30–60 s hold. Configurable via
  `rampMinThroughputRatio`.
- **Sustainability horizon.** "Sustainable" still means "over the measurement/hold window"; disk
  fill, GC, and compaction act on longer horizons. Not made worse by this change.

## References

- `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java` — verdict at
  `poll()` (current backlog-count check ~L166–187); bracket/chop machinery ~L194–293.
- `benchmark-framework/RATE_FINDING.md` — algorithm docs and the two "Trial finding" incidents.
- AKS run 30103329337 (`conduktor/benchmarks`) — the re-validation that produced the data above.

---

## Update (2026-07-29): AKS validation findings and revised direction — resource-headroom gate

Two AKS campaigns after the design above changed the conclusion. Recording them here because they
supersede the "THROUGHPUT verdict fixes the over-estimate" framing for at least one important class
of load.

### What the runs showed

1. **THROUGHPUT did not beat BACKLOG on a same-cluster comparison** (run 30256170166): `chop-throughput`
   confirmed 2,363 msg/s vs `chop-backlog` 2,383 — essentially identical, both far below AIMD's ~17k.
   The scale-free verdict was not the improvement it was designed to be on that cluster.

2. **A gateway confirmation campaign** (ramp = report10, fixed-rate confirm = report11) set each arm's
   target from a CHOP (`BACKLOG`) ramp, then re-ran at that fixed target with repeats:

   |     Arm     | CHOP target | Confirmed (sustained)  |    Gateway CPU (limit 2.0)     |      Verdict      |
   |-------------|-------------|------------------------|--------------------------------|-------------------|
   | direct      | 1,156,448   | ~1,165,446 (101%)      | ~0.02 cores (bypasses gateway) | sustainable       |
   | transparent | 1,177,897   | 1,088,637 (92%)        | —                              | close             |
   | encrypt     | 129,326     | 96,414 (75%), 46 s p99 | **2.0 pegged, throttled**      | **over-estimate** |

### Why `encrypt` over-estimated — it is NOT a verdict-signal problem

During the CHOP **ramp**, 129k for `encrypt` was clean by *every* producer/consumer signal, sustained
for ~7.5 min: backlog 0.6–5.9 K messages, publish-delay ~31 **microseconds** (flat), pub-latency
5–25 ms, gateway CPU **mean 1.47 / max 1.82 of 2.0 cores** (throttle 4.3/s). In the fixed-rate
**confirm**, the same 129k saturated: gateway 2.0 pegged (throttle ~10/s), backlog ~300 K,
publish-delay 6→60 s, achieved 96k.

Ruled out as the cause: **broker health** (broker CPU 0.3–0.5 cores and produce-time ~0 ms in both;
not degraded), **arm ordering** (encrypt ran last in both), **node hardware** (same dedicated
`aks-gateway` node pool / VM family in both).

Root cause: **129k sits at the very edge of the gateway's 2-core encryption budget.** Even in the
successful ramp the gateway peaked at 1.82/2.0 = **91%** and was already throttling. Encryption is
per-*message*-bound (2 pegged cores sustain only ~9.6 MB/s of 100-byte messages — <1% of raw AES-NI),
so per-message cost is high and nearly size-independent. At ~90–100% of a hard resource limit, small
run-to-run variance (GC/JIT/cgroup-throttle scheduling) flips the outcome sustainable ↔ saturated.

**Consequence for the design:** none of the producer/consumer-side gates — the `THROUGHPUT` ratio,
nor the proposed queue/latency/backlog-trend gates — could have caught this at ramp time, because the
CPU wall was *approached but not breached* (91%), so throughput, backlog, and delay were all
genuinely clean. Throughput-side signals cannot see a resource ceiling until it is crossed. The only
signal that revealed the fragility was the **gateway CPU utilization itself (91%, throttling)**, which
`RampRateFinder` never sees — it consumes only worker counters.

### Revised direction: gate on bottleneck-resource headroom

Reject or derate a candidate whose **bottleneck resource utilization** during the confirming hold
exceeds a headroom threshold (e.g. 85%), independent of the throughput/backlog/latency signals.

Two ways to apply it:

- **Harness-side, post-hoc (recommended; no CHOP change).** `RampRateFinder` already emits
  `rampVerification` with the confirmation-window timestamps, and the harness already captures
  `chop-verify.metrics.json` — a Prometheus resource sample over exactly that window. The harness
  computes peak (or p95) utilization of the bottleneck resource (gateway `container_cpu_usage /
  cpu_limit_cores`) and: (a) **flags** the result when > threshold ("confirmed at 91% gateway CPU —
  low headroom, may not reproduce"), and/or (b) **derates** the recommended target to leave headroom:
  `target' = target × (headroom_target / observed_peak_util)`. For encrypt that is
  `129k × (0.85 / 0.91) ≈ 120k`, and ideally a short re-confirm at 120k. This keeps OMB generic (it
  never learns about "gateway CPU"), uses data already collected, and needs no change to the finder.

- **In-loop (larger change).** Give the finder a pluggable `ResourceHeadroomProvider` — a callback
  returning current bottleneck utilization [0,1], default a no-op returning 0 — and have the verdict
  fail/derate when utilization exceeds the threshold. This lets headroom shape *discovery* (bracket
  stops climbing once the resource nears its limit) rather than only correcting the reported number
  after the fact. More invasive and adds a live-scrape dependency; the harness injects the provider,
  so OMB stays infra-agnostic.

The bottleneck resource is topology-dependent (gateway CPU for `encrypt`; broker CPU/disk/NIC for
`direct`). The harness knows the topology, which is another reason the harness-side application is the
natural home. This gate is complementary to — not a replacement for — the `THROUGHPUT` verdict: the
verdict addresses scale/count blindness; the headroom gate addresses confirming rates that sit at the
edge of a hard resource limit.

### References (this update)

- report10 (`conduktor/benchmarks`) — CHOP `BACKLOG` ramp that set the confirmation targets.
- report11 — fixed-rate confirmation with repeats (direct/transparent/encrypt).

