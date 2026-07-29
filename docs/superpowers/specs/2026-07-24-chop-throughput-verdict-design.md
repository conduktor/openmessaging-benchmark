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

   |     Arm     | CHOP target |   Whole-window mean    | Steady tail (last 100s) |      Gateway CPU (limit 2.0)      |      Verdict      |
   |-------------|-------------|------------------------|-------------------------|-----------------------------------|-------------------|
   | direct      | 1,156,448   | ~1,165,446 (101%)      | 1,158,515 (100%)        | ~0.02 cores (bypasses gateway)    | sustainable       |
   | transparent | 1,177,897   | 1,088,637 (92%)        | **1,186,455 (101%)**    | 0.71–0.84, **no** throttling      | **sustainable**   |
   | encrypt     | 129,326     | 96,414 (75%), 46 s p99 | ~103,946 (80%) plateau  | **1.998 pegged, throttled ~10/s** | **over-estimate** |

   The transparent row was originally recorded as a 92% near-miss ("close"). It is not: the arm sustained
   *above* target once warm, and its whole-window mean is dragged under 0.95× by a ~90-second cold-start
   ramp during which nothing is saturated (median publish latency 14–17 ms, zero backlog, gateway
   unthrottled). Only encrypt is a real over-estimate. A confirm harness that averages across the ramp
   will misclassify any arm whose warmup is shorter than its time-to-steady-state.

### Why `encrypt` over-estimated — it is NOT a verdict-signal problem

During the CHOP **ramp**, 129k for `encrypt` was clean by *every* producer/consumer signal, sustained
for ~7.5 min: backlog 0.6–5.9 K messages, publish-delay ~31 **microseconds** (flat), pub-latency
5–25 ms. It then ran a *600-second* measurement window at the same rate and sustained 100.0% of it. In
the fixed-rate **confirm**, the same 129k saturated: gateway 1.998 pegged (throttle ~10/s), backlog
~300 K, publish-delay 6→60 s (clipped at the histogram ceiling), achieved 96k.

Gateway CPU over the **confirming hold** — the window a headroom gate would inspect, and the figures to
use when sizing one — was **median 1.690 / peak 1.807 of 2.0 cores (84.5% / 90.3%)**, throttling at
~1.9 CFS periods/s. (An earlier draft quoted "mean 1.47 / max 1.82, throttle 4.3/s"; those are whole-ramp
statistics spanning the low-rate bracket holds and the *failed* 136k hold, which both understates the
utilisation at the confirmed rate and overstates the peak attributable to it.)

Ruled out as the cause: **broker health** (broker CPU 0.3–0.5 cores and produce-time ~0 ms in both; not
degraded, and 98–99% request-handler idle), **arm ordering** (encrypt ran last in both), and **workers /
network** (the non-gateway `direct` arm was unaffected — marginally *faster* in the confirm run).

**Not** ruled out: **node hardware.** The original entry dismissed this on the grounds of "same dedicated
`aks-gateway` node pool / VM family", which does not follow — the two runs are different physical clusters
(`aks-gateway-21146700` vs `aks-gateway-22649591`), so same pool config and VM family but a different
host. This is the hypothesis the data most supports, and it is the one that was excluded without evidence.

Root cause: **129k sits close enough to the gateway's 2-core encryption budget that a modest change in
per-message CPU cost consumes the whole margin.** Encryption is per-*message*-bound (2 pegged cores
sustain only ~9.6 MB/s of 100-byte messages — <1% of raw AES-NI), so per-message cost is high and nearly
size-independent. Report10's CPU-vs-rate curve for this arm is clean and linear at **~76,000 msg/s per
core** across 80k–136k (0.4% spread). Report11's gateway needed *more than* 2.0 cores to do what
report10's did in 1.69 — a regression of **≥18%**, with no measurable upper bound because throttling
censors demand above the quota. It was already throttled in its **first sample**, while publish delay was
still 2.2 ms and no back-pressure existed, so the CPU wall is the cause and not a consequence of the
collapse. The rest follows arithmetically: gateway latency >1 s → the producer's 15 in-flight requests ×
~9,523 records ÷ 1.19 s ≈ 120k ceiling → `buffer.memory` exhausted → `bufferpool_wait_rate` 0.91 → ~96k.

**Consequence for the design:** none of the producer/consumer-side gates — the `THROUGHPUT` ratio,
nor the proposed queue/latency/backlog-trend gates — could have caught this at ramp time, because the
CPU wall was *approached but not breached* (84.5% median), so throughput, backlog, and delay were all
genuinely clean. Throughput-side signals cannot see a resource ceiling until it is crossed. The only
signal that revealed the fragility was the **gateway CPU utilization itself**, which `RampRateFinder`
never sees — it consumes only worker counters.

### Revised direction: gate on bottleneck-resource headroom

Reject or derate a candidate whose **bottleneck resource utilization** during the confirming hold
exceeds a headroom threshold, independent of the throughput/backlog/latency signals.

**Two corrections to the threshold, from checking it against the run it was designed to reject.** An
earlier draft of this section proposed 85%, gated on peak-or-p95, with a derate to ~120k. Neither survives
contact with `encrypt.chop-verify.metrics.json` — the very file this design says the harness should
consume:

- **The median over the confirming hold is 84.5%, so an "85%" gate PASSES the rate that failed to
  reproduce.** Only the peak (90.3%) rejects it. If the threshold is 85%, the statistic must be the peak
  or a high percentile; "peak (or p95)" is not an implementation detail here, it decides the outcome.
- **85% is itself too high, and the proposed derate does not survive either.** Pricing each candidate
  against the measured ~76,000 msg/s/core curve:

  |                 target                  | cores | % of 2-core limit | within report11's ~104k plateau? |
  |-----------------------------------------|-------|-------------------|----------------------------------|
  | 129,326 (confirmed)                     | 1.690 | 84.5%             | no                               |
  | 120,000 (`129k × 0.85/0.91`, this spec) | 1.568 | 78.4%             | **no**                           |
  | 103,946 (report11's post-stall plateau) | 1.358 | **67.9%**         | at the limit, by construction    |
  | 100,570 (report11's final interval)     | 1.314 | 65.7%             | yes                              |

  To have produced a number that reproduced, the gate would have had to reject anything above **~65–68%**.

  Report11's encrypt run is not a monotone decline, and its summary statistics mislead accordingly: it
  declines (135k → 87k over 18 intervals), **stalls near-completely for ~50 s** to a low of 942 msg/s, then
  recovers to a ~104k plateau. Its whole-window mean of 96,414 — and any "last N intervals" average that
  straddles the stall — understate the plateau, which is the defensible capacity figure.

The general form matters more than the constant: ~65% is calibrated to one observed cross-cluster delta
(≥18%), so the threshold belongs expressed as **required margin ≥ measured node-to-node variance**. A
fixed 85% silently encodes an assumption of ~15% variance that this campaign refutes. This also means the
threshold cannot be calibrated from a single cluster — it needs the same arm on two cluster instances,
which report10 and report11 only provided by accident.

**And read utilisation only over an otherwise-clean hold.** Past the knee the signal inverts: during that
50-second stall the gateway's CPU *fell* to 0.273 cores, because the producer had wedged on `buffer.memory`
and stopped feeding it. A gate averaging utilisation over a window containing such a stall would read low
utilisation as ample headroom. The confirming hold is the right window precisely because it is otherwise
clean, which is an argument against generalising the gate to arbitrary windows.

Two ways to apply it:

- **Harness-side, post-hoc (recommended; no CHOP change).** `RampRateFinder` already emits
  `rampVerification` with the confirmation-window timestamps, and the harness already captures
  `chop-verify.metrics.json` — a Prometheus resource sample over exactly that window. The harness
  computes peak (or p95) utilization of the bottleneck resource (gateway `container_cpu_usage /
  cpu_limit_cores`) and: (a) **flags** the result when > threshold ("confirmed at 90% peak gateway CPU —
  low headroom, may not reproduce"), and/or (b) **derates** the recommended target to leave headroom:
  `target' = target × (headroom_target / observed_peak_util)`. With the corrected threshold that is
  `129k × (0.65 / 0.903) ≈ 93k` for encrypt, plus a short re-confirm at the derated rate. This keeps OMB
  generic (it never learns about "gateway CPU"), uses data already collected, and needs no change to the
  finder. Note the derate is only as good as `headroom_target`, so it inherits the calibration problem
  above — a re-confirm at the derated rate is what actually establishes the number, not the arithmetic.

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

