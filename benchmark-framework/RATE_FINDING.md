# Rate finding (`producerRate: 0`)

Setting `producerRate: 0` in a workload YAML tells the benchmark to discover the maximum
sustainable producer rate itself, instead of running at a fixed rate you supply. Two algorithms
implement this, selected via the `rampAlgorithm` workload field:

|                                       |  `rampAlgorithm: AIMD` (default)  |       `rampAlgorithm: CHOP`        |
|---------------------------------------|-----------------------------------|------------------------------------|
| Status                                | Original/mainline behavior        | Opt-in addition                    |
| Behavior                              | Continuous, never stops adjusting | Converges, then holds a fixed rate |
| Configuration                         | Environment variables only        | Workload YAML fields               |
| Verifies the rate before reporting it | No                                | Yes (hold-and-confirm)             |
| Detects an unreliable/flaky result    | No                                | Yes (`isNonMonotonic`)             |
| Result attached to output JSON        | No                                | `rampVerification` object          |

## AIMD (`RateController.java`, `WorkloadGenerator.findMaximumSustainableRate()`)

This is the original discovery mechanism and remains the default; nothing about it changed when
`CHOP` was added.

When `producerRate == 0` and `rampAlgorithm == AIMD` (or unset), `WorkloadGenerator.run()` starts
`targetPublishRate` at a hardcoded `10000` msg/s and spawns a **background thread** that runs
`findMaximumSustainableRate()` for the rest of the test — including through warmup and the entire
measurement window, only stopping when the test itself ends.

That thread loops every `controlPeriodMillis = 3000` (hardcoded, 3 seconds):

1. Read cumulative `messagesSent`/`messagesReceived` from the worker.
2. Call `RateController.nextRate(currentRate, periodNanos, sent, received)`.
3. Apply the returned rate immediately via `worker.adjustPublishRate(...)`.

`RateController` implements the actual control logic — an additive-increase/multiplicative-decrease
(AIMD) style controller built around a `rampingFactor`:

- Compute `expected` throughput for the period from the current rate, and compare it against what
  was actually `published`/`received`.
- If `receiveBacklog` (`subscriptionsPerTopic * totalPublished - totalReceived`) or `publishBacklog`
  (`expected - published`) exceeds a limit → **ramp down**: halve `rampingFactor` (floored at
  `minRampingFactor`) and return a reduced rate computed from actual throughput.
- Otherwise → **ramp up**: double `rampingFactor` (capped at `maxRampingFactor`) and return
  `rate + rate * rampingFactor` — exponential growth as long as no backlog builds up.

Its tuning knobs are **environment variables only** — there is no way to set them per-workload in
YAML:

|         Env var         | Default |                             Meaning                              |
|-------------------------|---------|------------------------------------------------------------------|
| `PUBLISH_BACKLOG_LIMIT` | 1000    | Messages of publish-side lag tolerated before ramping down       |
| `RECEIVE_BACKLOG_LIMIT` | 1000    | Messages of consumer lag tolerated before ramping down           |
| `MIN_RAMPING_FACTOR`    | 0.01    | Floor for the ramping factor after repeated ramp-downs           |
| `MAX_RAMPING_FACTOR`    | 1.0     | Ceiling for the ramping factor (i.e. max rate growth per period) |

**What this means in practice**: AIMD never "finishes" and never claims the number it's currently
using is verified. Whatever rate happens to be in effect when the measurement window's stats are
collected is what gets reported — it could be mid-ramp-up, mid-ramp-down, or oscillating. There's
no equivalent of "this rate was confirmed sustainable"; there's only "this is what the reactive
loop was doing at the moment we looked."

## CHOP (`RampRateFinder.java`, `WorkloadGenerator.runChopDiscovery()`)

CHOP exists because AIMD's constant oscillation makes its answer hard to reproduce: a rate that
looked fine for one 3-second window isn't necessarily one you can safely run for a full test.
Opt in with `rampAlgorithm: CHOP` in the workload YAML.

Unlike AIMD, CHOP runs to completion **before** warmup starts (`runChopDiscovery()` blocks
`WorkloadGenerator.run()`), then the measurement window runs at the fixed rate CHOP converged on.

### Algorithm

1. **Settle** — run at `rampStartRate` for `rampSettleSeconds` (default 30s) before evaluating
   backlog *at all*. Right after `startLoad()`, a consumer-group rebalance tail or producer
   connection warm-up can still be settling; without this grace period, that transient gets
   evaluated exactly like a real capacity problem and can permanently cap the search too low (see
   "Known limitation" below).
2. **Bracket** — starting at `rampStartRate`, double the rate every poll while backlog stays
   clean. A breach is immediately actionable (fail-fast, no need to hold out a rate that's already
   failing) and sets a known-bad `hi`; a clean reading must hold for `rampBracketHoldSeconds`
   (defaults to the resolved `rampHoldSeconds` — i.e. bracket is exactly as rigorous as chop unless
   you deliberately shorten it) before being accepted as the known-good `lo`. (Handles the reverse
   case too — if even the start rate is already overloaded, it halves downward until it finds a
   clean `lo`.)
3. **Chop** — binary search the `[lo, hi]` bracket. Each candidate is held for `rampHoldSeconds`
   (default 30s), not just glanced at — a single reactive snapshot (AIMD's approach) isn't enough
   to know a rate actually holds.
4. **Confirm** — once a candidate holds clean within `rampConvergenceTolerance` (default 5%), it
   isn't accepted immediately. It must pass `rampConfirmationHolds` (default 1) additional,
   consecutive clean holds before being accepted. This is what makes "verified" mean something more
   than "passed once."

   Those confirmation holds run at **`lo × (1 − rampConvergenceTolerance)`**, not at `lo` itself.
   Chop converges to within the tolerance of a *failing* rate, so `lo` sits right at the knee: it is
   the highest rate observed to pass, but the search only knows the knee to within the tolerance, so
   `lo` can be fractionally above the real ceiling — and under `THROUGHPUT` it provably can be, since
   that gate accepts anything up to `capacity / rampMinThroughputRatio`. Confirming just below the
   knee has two effects: the re-check stops being a coin flip at the boundary (a flip there used to
   latch `isNonMonotonic()` and withhold the whole result), and the rate finally reported is one that
   was actually held for a full hold. Note the coupling: `rampConvergenceTolerance` therefore doubles
   as the size of the safety margin.

5. **Reopen on a failed confirmation** — if a confirmation hold fails (the rate looked fine, then
   didn't hold up), CHOP does not accept it anyway. It reopens the search: tightens `hi` to the
   failed rate, and falls back to the highest rate already *observed* to pass below it (never a
   blind guess — `lo` is always sourced from a real, recorded pass) as the new `lo`, then restarts
   the hold-and-confirm cycle from there.

6. **Non-monotonic flag** — every tested `(rate, passed/failed)` outcome is recorded. If a later
   verdict ever contradicts an earlier one (e.g. a lower rate fails after a higher one already
   passed), `isNonMonotonic()` is set and stays set for the rest of the run, even if the reopened
   search goes on to confirm cleanly. It's a signal that the system showed unstable behavior
   *somewhere* during discovery, so the final number may not reproduce as cleanly as a clean run
   would.

7. **Safety cap** — `rampMaxDiscoveryMinutes` (default 10) bounds total discovery time; if hit,
   discovery stops and reports the best confirmed-or-passed `lo` found so far. Bear in mind the
   settle period and every bracket/chop hold all draw from this same budget — with generous hold
   settings, raise this alongside them.

### Verdict modes

Each hold (bracket or chop) needs a clean/not-clean verdict before CHOP can act on it. Which
predicate decides that is controlled by `rampVerdict`, with two modes:

- **`BACKLOG`** (default) — the predicate described above: a hold is clean if `receiveBacklog`/
  `publishBacklog` never exceeds the configured limit (`rampMaxBacklogSeconds`/`Floor`/`Ceiling`,
  or the fixed `rampPublishBacklogLimit`/`rampReceiveBacklogLimit` when the relative one is unset).
  Structural weakness: a healthy pipeline's in-flight backlog scales with throughput, so any
  fixed-ish count is too strict at high rates and too loose at low ones — see the two "Trial
  finding" sections below, both of which are this predicate misfiring in opposite directions.
- **`THROUGHPUT`** (opt-in) — a scale-free alternative. Per hold: `clean ⇔ published ≥
  ratio·expected AND received ≥ ratio·published`, where `ratio = rampMinThroughputRatio` (default
  0.95). It checks that the pipeline both kept up with the target rate *and* drained what it
  published, as ratios rather than message counts, so the same threshold applies unchanged whether
  the candidate rate is 100 msg/s or 1,000,000 msg/s. There's no per-poll fast-fail — the verdict
  is decided once, at hold completion, from the hold's aggregate published/received counts, not
  from intermediate backlog snapshots. It does **not** solve hold-length sensitivity: a hold still
  has to run longer than the broker's burst-absorption time in either mode, or a genuinely
  unsustainable rate can still look clean for the duration of a too-short hold.

### Configuration (workload YAML fields, all optional, only apply when `producerRate: 0`)

|           Field            |                Default                 |                                                                                                                                                                                 Applies to                                                                                                                                                                                 |
|----------------------------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rampAlgorithm`            | `AIMD`                                 | Selects the algorithm                                                                                                                                                                                                                                                                                                                                                      |
| `rampVerdict`              | `BACKLOG`                              | CHOP only — selects the clean/not-clean predicate for holds; `THROUGHPUT` is the scale-free alternative (see "Verdict modes" above). AIMD never builds a `RampRateFinder`, so this field has no effect on it                                                                                                                                                               |
| `rampMinThroughputRatio`   | 0.95                                   | CHOP only — THROUGHPUT verdict                                                                                                                                                                                                                                                                                                                                             |
| `rampStartRate`            | 10000                                  | Both                                                                                                                                                                                                                                                                                                                                                                       |
| `rampPublishBacklogLimit`  | env `PUBLISH_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                                                                       |
| `rampReceiveBacklogLimit`  | env `RECEIVE_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                                                                       |
| `rampMaxBacklogSeconds`    | unset                                  | CHOP only — when set, replaces the two fields above with a limit that scales with the candidate rate (`limit = clamp(currentRate * rampMaxBacklogSeconds, rampMaxBacklogFloor, rampMaxBacklogCeiling)`), so the check is equally strict at every rate tried during bracket's exponential range instead of being loose at high rates and tight at low ones                  |
| `rampMaxBacklogFloor`      | 1000                                   | CHOP only — hard floor (messages) on the limit `rampMaxBacklogSeconds` computes, matching the old fixed-count default so the relative check can never become stricter than a fixed-count check would have been at any rate; only meaningful when `rampMaxBacklogSeconds` is set (see the trial log)                                                                        |
| `rampMaxBacklogCeiling`    | 100000                                 | CHOP only — hard cap (messages) on the same limit; only meaningful when `rampMaxBacklogSeconds` is set (see the trial log for why this exists)                                                                                                                                                                                                                             |
| `rampBracketPeriodSeconds` | 3                                      | CHOP only — poll cadence, not a hold duration                                                                                                                                                                                                                                                                                                                              |
| `rampSettleSeconds`        | 30                                     | CHOP only — grace period at start before backlog counts at all                                                                                                                                                                                                                                                                                                             |
| `rampBracketHoldSeconds`   | resolved `rampHoldSeconds`             | CHOP only — how long a bracket candidate must hold clean; shorten independently once you trust bracket's coarser candidates need less scrutiny                                                                                                                                                                                                                             |
| `rampDrainSeconds`         | 0 (off)                                | CHOP only — cap on the recovery period run after a *failed* candidate, at the highest rate already known to hold. Ends as soon as the backlog is back within the limit it is judged against, so it costs one poll when there is nothing to drain. Opt-in because enabling it changes the search trajectory after every failure; `rampHoldSeconds` is a good starting value |
| `rampHoldSeconds`          | 30                                     | CHOP only — how long a chop candidate must hold clean                                                                                                                                                                                                                                                                                                                      |
| `rampConfirmationHolds`    | 1                                      | CHOP only                                                                                                                                                                                                                                                                                                                                                                  |
| `rampConvergenceTolerance` | 0.05                                   | CHOP only                                                                                                                                                                                                                                                                                                                                                                  |
| `rampMaxDiscoveryMinutes`  | 10                                     | CHOP only                                                                                                                                                                                                                                                                                                                                                                  |

See `workloads/max-rate-chop-1-topic-100-partitions-100b.yaml` for a runnable example.

### Output

When discovery ends in a genuine confirm *and* nothing during discovery ever contradicted anything
else, the result JSON gets an extra `rampVerification` object — absent from AIMD runs, from CHOP
runs that only hit the safety cap, and from CHOP runs that flagged `isNonMonotonic()` at any point
(see the trial log — a contradicted discovery is withheld entirely rather than reported as
a specific, possibly-unreproducible rate):

```json
"rampVerification": {
  "rate": 46000.0,
  "startEpochMillis": 1784817684100,
  "endEpochMillis": 1784817690849,
  "nonMonotonic": false
}
```

`rate` is the throughput the confirmation hold **actually achieved** (`published / holdSeconds`),
not the target it was asked for. The two agree at a sustainable rate; where they disagree, the
achieved figure is the true one, and it is the figure the window below actually brackets. Because
confirmation runs at `lo x (1 - rampConvergenceTolerance)` (see "Confirm" above), this number is
deliberately on the conservative side of the knee — read it as "a rate you can run", not as a
capacity ceiling.

`nonMonotonic` is therefore always `false` whenever this object is present (kept in the schema for
stability rather than removed). `startEpochMillis`/`endEpochMillis` bracket the confirmation holds —
useful for attributing resource usage (CPU/memory) to the verified rate rather than the whole warmup
+ discovery + measurement run. With `rampConfirmationHolds > 1` the window spans all of them, not
just the last.

### When discovery refuses to answer, and how to see why

Discovery used to be able to return a number in situations where it had not actually found one. It
now fails loudly instead:

- **Rejected up front** (`IllegalArgumentException` from the `WorkloadGenerator` constructor):
  `producerRate: 0` with `subscriptionsPerTopic` or `consumerPerSubscription` at `0` — with no
  consumers there is no drain signal, so no candidate can ever be judged sustainable. Both are
  primitive `int`s and the YAML mapper ignores unknown properties, so an omitted *or misspelled*
  field silently arrives as `0`. Also rejected: `rampMinThroughputRatio` outside `(0, 1]`,
  `rampConvergenceTolerance` outside `(0, 1)`, and `rampMaxBacklogFloor` above
  `rampMaxBacklogCeiling` (the limit is `max(floor, min(rate x seconds, ceiling))`, so the floor
  would silently win and the ceiling never apply).
- **Nothing ever held** (`IllegalStateException`): the bracket phase halves on every failure, so if
  no candidate holds cleanly it bottoms out at `rampStartRate / 2^20` with no `lo` to report.
  `LocalWorker` clamps anything under `1.0` to `1 msg/s`, so this previously ran the whole
  measurement window at a flatline and certified it — the shape of the `0.9155 msg/s` incident in the
  trial log.
- **Out of time** (`WARN`, not a failure): hitting `rampMaxDiscoveryMinutes` reports the best rate
  that held but *without* a confirmation, which otherwise looks identical to a converged run from
  the outside. Worth watching under `THROUGHPUT`, which has no per-poll fast-fail, so every
  candidate — including doomed bracket overshoots — costs a full hold. Budget roughly
  `settle + (bracket steps + chop steps + confirmation holds) x hold`.

For diagnosis, discovery logs its **resolved** configuration once at start (every ramp field defaults
silently, so a misspelled one otherwise looks like it applied), and one `FINDER-HOLD` line per
completed hold at `INFO`:

```
FINDER-HOLD phase=CHOP rate=4750.0 verdict=exceeded confirming=false expected=4750 published=4500
            received=4500 achievedRatio=0.947 drainRatio=1.000 bracket=[4500.0, 5000.0]
```

`achievedRatio` is `published / expected` and `drainRatio` is
`received / (subscriptionsPerTopic x published)` — the two quantities the `THROUGHPUT` verdict
compares against `rampMinThroughputRatio`, shown in both modes.

### Known limitation

Bracket phase deliberately overshoots (2x) past the real limit before backing off, to find `hi`.
That overshoot's fallout (backlog, GC pressure, broker load) is not drained before the next
candidate starts its hold clock — so a reading can be affected by the previous candidate's overshoot
rather than reflecting steady state at the candidate under test. This matters more than it sounds:
the bracket phase only ever moves `lo` *upward*, so a single contaminated reading cannot be recovered
from later.

**Addressable via `rampDrainSeconds`** (opt-in, default off). When set, a failed candidate is followed
by a recovery period at the highest rate already observed to hold — a rate the consumers are known to
keep up with, so queues actually shrink, which draining at the *next candidate* would not guarantee
since that candidate may itself be above capacity. Nothing is evaluated during recovery and the
counters are re-baselined, exactly as `rampSettleSeconds` does for start-up transients. It is a cap
rather than a fixed wait: recovery ends as soon as the backlog is back within the limit it is judged
against, so when there is nothing to drain it costs a single poll.

Note what this does and does not fix. It removes *cross-candidate* contamination. It does not make a
history-dependent verdict into a function of rate — see the trial log, where the same 800k candidate
returned opposite verdicts on the same broker minutes apart.

### Caveat when reading a ramp run's reported rates

`LocalWorker.resetStats()` calls only `stats.resetLatencies()`, never `stats.reset()` — so the
message counters are *not* cleared between discovery and the measurement window. The first 10-second
interval of `printAndCollectStats` therefore attributes everything published during discovery to that
one window. On a `producerRate: 0` run this was observed reporting **15,439,670 msg/s** for the first
interval against a steady-state **481,000 msg/s**: roughly six minutes of discovery folded into one
sample.

So the first entry of `publishRate` / `consumeRate` (and the byte-rate equivalents) is junk on any
`producerRate: 0` run, AIMD included, and a mean over those lists inherits it — use the median, or
drop the first sample. Latency figures are unaffected: `resetLatencies()` does clear both the
interval and cumulative recorders, so `aggregatedPublishDelayLatencyAvg` and the quantiles describe
the measurement window only. Not fixed here because `WorkerStats.reset()` also clears
`totalMessagesSent`/`totalMessagesReceived`, which the backlog-drain logic depends on.

### Trial log

The specific runs behind several of the guards above -- the two backlog-tolerance incidents, the
~320x under-report that motivated `THROUGHPUT`, and the hold-length finding -- are recorded in
[`docs/ramp-finder-trial-log.md`](../docs/ramp-finder-trial-log.md). That file is working notes:
useful if you are changing the finder or wondering why a default is what it is, not needed to run one.

## Which to use

AIMD if you just want *a* number and don't care whether it's reproducible in a follow-up run at a
fixed rate. CHOP if you intend to take the discovered rate and actually rely on it — e.g. feeding
it into a fixed-`producerRate` workload later, or attributing resource cost to "the rate we
verified" rather than an average blurred across a whole oscillating test.
