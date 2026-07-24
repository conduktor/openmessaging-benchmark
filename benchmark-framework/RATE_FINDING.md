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

Neither algorithm's mechanics were documented anywhere before this file — there's no README or
`docs/` entry for either. This is that documentation.

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
- If `receiveBacklog` (`totalPublished - totalReceived`) or `publishBacklog`
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
   "Known limitation" below — this is the fix for it).
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
   consecutive clean holds at the *same* rate before being accepted. This is what makes "verified"
   mean something more than "passed once."
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

### Configuration (workload YAML fields, all optional, only apply when `producerRate: 0`)

|           Field            |                Default                 |                                                                                                                                                             Applies to                                                                                                                                                             |
|----------------------------|----------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rampAlgorithm`            | `AIMD`                                 | Selects the algorithm                                                                                                                                                                                                                                                                                                              |
| `rampStartRate`            | 10000                                  | Both                                                                                                                                                                                                                                                                                                                               |
| `rampPublishBacklogLimit`  | env `PUBLISH_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                               |
| `rampReceiveBacklogLimit`  | env `RECEIVE_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                               |
| `rampMaxBacklogSeconds`    | unset                                  | CHOP only — when set, replaces the two fields above with a limit that scales with the candidate rate (`limit = min(currentRate * rampMaxBacklogSeconds, rampMaxBacklogCeiling)`), so the check is equally strict at every rate tried during bracket's exponential range instead of being loose at high rates and tight at low ones |
| `rampMaxBacklogCeiling`    | 100000                                 | CHOP only — hard cap (messages) on the limit `rampMaxBacklogSeconds` computes; only meaningful when `rampMaxBacklogSeconds` is set (see "Trial finding" below for why this exists)                                                                                                                                                 |
| `rampBracketPeriodSeconds` | 3                                      | CHOP only — poll cadence, not a hold duration                                                                                                                                                                                                                                                                                      |
| `rampSettleSeconds`        | 30                                     | CHOP only — grace period at start before backlog counts at all                                                                                                                                                                                                                                                                     |
| `rampBracketHoldSeconds`   | resolved `rampHoldSeconds`             | CHOP only — how long a bracket candidate must hold clean; shorten independently once you trust bracket's coarser candidates need less scrutiny                                                                                                                                                                                     |
| `rampHoldSeconds`          | 30                                     | CHOP only — how long a chop candidate must hold clean                                                                                                                                                                                                                                                                              |
| `rampConfirmationHolds`    | 1                                      | CHOP only                                                                                                                                                                                                                                                                                                                          |
| `rampConvergenceTolerance` | 0.05                                   | CHOP only                                                                                                                                                                                                                                                                                                                          |
| `rampMaxDiscoveryMinutes`  | 10                                     | CHOP only                                                                                                                                                                                                                                                                                                                          |

See `workloads/max-rate-chop-1-topic-100-partitions-100b.yaml` for a runnable example.

### Output

When discovery ends in a genuine confirm *and* nothing during discovery ever contradicted anything
else, the result JSON gets an extra `rampVerification` object — absent from AIMD runs, from CHOP
runs that only hit the safety cap, and from CHOP runs that flagged `isNonMonotonic()` at any point
(see "Trial finding" below — a contradicted discovery is withheld entirely rather than reported as
a specific, possibly-unreproducible rate):

```json
"rampVerification": {
  "rate": 46000.0,
  "startEpochMillis": 1784817684100,
  "endEpochMillis": 1784817690849,
  "nonMonotonic": false
}
```

`nonMonotonic` is therefore always `false` whenever this object is present (kept in the schema for
stability rather than removed). `startEpochMillis`/`endEpochMillis` bracket the *final* confirmation
hold specifically — useful for attributing resource usage (CPU/memory) to the verified rate rather
than the whole warmup + discovery + measurement run.

### Known limitation

Bracket phase deliberately overshoots (2x) past the real limit before backing off, to find `hi`.
That overshoot's fallout (backlog, GC pressure, broker load) isn't drained before the first chop
candidate starts its hold clock — so an early chop reading can still be affected by bracket's own
overshoot rather than reflecting steady state at that candidate alone. Not yet addressed; would
need an explicit drain sub-phase between bracket and chop (distinct from `rampSettleSeconds`
above, which only runs once, before bracket starts).

This used to also apply to the very *first* bracket reading: a consumer-group rebalance tail or
producer connection warm-up still settling right after `startLoad()` could be evaluated exactly
like a real capacity problem, permanently capping `hi` far below the system's actual ceiling — one
bad early reading, and chop would only ever search inside the too-small bracket it produced, with
no way to recover (unlike AIMD, which just keeps retrying for the rest of the test). `rampSettleSeconds`
and requiring bracket's clean readings to hold (`rampBracketHoldSeconds`) fixed that specific case.

### Trial finding: `rampMaxBacklogSeconds` can defeat its own backlog check at high rates

Found running the consuming harness's (`conduktor/benchmarks`) AKS integration test against this
branch's image, 2026-07-24. Reported here (not just as harness-side feedback) because it looks
like a real gap in this algorithm, independent of the harness.

**Setup**: AKS, `representative` infra preset (dedicated node pools, node-isolated brokers/gateway/
workers), direct-to-Kafka (no gateway), 1 topic / 100 partitions / 100-byte messages, single
producer/consumer. Two loads on the same topology for comparison: `rampAlgorithm: AIMD` (default)
and `rampAlgorithm: CHOP` with:

```yaml
rampStartRate: 20000
rampMaxBacklogSeconds: 1.0
rampBracketHoldSeconds: 30
rampHoldSeconds: 60
rampConvergenceTolerance: 0.05
rampMaxDiscoveryMinutes: 12
testDurationMinutes: 15
```

(`rampBracketPeriodSeconds` and `rampSettleSeconds` left at their defaults, 3s and 30s.)

**Result**: CHOP reported a genuine confirm —

```
WARN  WorkloadGenerator - Ramp discovery detected non-monotonic backlog behavior --
      1800000.0 msg/s may not reproduce reliably; consider treating it as a band rather
      than an exact figure.
INFO  WorkloadGenerator - ----- Ramp discovery (CHOP) complete: 1800000.0 msg/s -----
```

`rampVerification.rate = 1800000.0`, `nonMonotonic = true`. That is not a plausible sustainable
rate for this setup: **AIMD topped out at ~28,000 msg/s** on the identical topology/hardware in
the same run (a separate trial on the same cluster shape previously saw AIMD peak ~65,000), and
CHOP's own subsequent fixed-rate measurement window — running at the "confirmed" 1,800,000 target —
only *achieved* ~1.3-1.5M, i.e. it couldn't even sustain the number it had just certified. Off by
roughly two orders of magnitude from anything AIMD ever observed.

**Diagnosis**: `rampMaxBacklogSeconds` scales the backlog tolerance *with the candidate rate*
(`limit = currentRate * rampMaxBacklogSeconds`) — correct in spirit (fixed-count limits are loose
at high rates, tight at low ones), but nothing bounds how large that limit gets as bracket's
exponential doubling runs away. At a 1,000,000 msg/s candidate, `rampMaxBacklogSeconds: 1.0` means
tolerating **1,000,000 messages** of backlog before flagging a problem — by that point the check is
essentially disabled. The bracket phase (or a reopened search) apparently kept finding *some*
combination of candidates that looked clean under that ballooning tolerance, occasionally
contradicting each other (hence `nonMonotonic: true` — the flag did its job and caught that
something was wrong), but the search still landed on and reported a concrete, confidently-wrong
number rather than stopping or refusing to confirm.

**Suggested follow-ups** (not implemented here — flagging for whoever picks this up):
- Cap the rate-relative limit with an absolute ceiling too, e.g.
`min(currentRate * rampMaxBacklogSeconds, someAbsoluteMax)`, so it can't grow unbounded during
exponential bracket doubling.
- Consider treating `nonMonotonic: true` as more than an advisory flag — e.g. refusing to attach
`rampVerification` at all (or attaching a band instead of a point value) when the discovery that
produced it was internally contradictory, rather than reporting a specific rate that the
docstring itself says "may not reproduce reliably."
- Document a **guideline value** for `rampMaxBacklogSeconds` (this trial used 1.0s, which in
hindsight is far too generous once compounded with rate — something like 0.05-0.1s is probably
closer to what fixed-count defaults were achieving at realistic rates) rather than leaving the
right order of magnitude to guesswork.

Harness-side note (not a CHOP issue): the resource-sampling integration this trial was validating
worked correctly regardless — a `rampVerification`-scoped Prometheus capture was produced
alongside the whole-job one, with sensible (lower) CPU/mem numbers in the narrower window. The bad
data here is a confirmed *rate*, not a broken capture mechanism.

**Fixed**: both suggested follow-ups above are now implemented.
`rampMaxBacklogCeiling` (default 100000) caps the relative limit so it can't grow unbounded as
bracket's exponential doubling runs away, and `isNonMonotonic()` is no longer purely advisory —
`rampVerification` is now withheld entirely (not attached, at any rate) whenever discovery flagged
non-monotonic behavior anywhere, confirmed or not, rather than reporting a specific number the log
message itself admits may not reproduce. The third suggestion (a documented guideline value for `rampMaxBacklogSeconds` itself) remains
open — this trial used `1.0`, and while the ceiling now guards against that value running away, it
was still a somewhat arbitrary starting point rather than one derived from what fixed-count limits
were actually achieving at realistic rates.

## Which to use

AIMD if you just want *a* number and don't care whether it's reproducible in a follow-up run at a
fixed rate. CHOP if you intend to take the discovered rate and actually rely on it — e.g. feeding
it into a fixed-`producerRate` workload later, or attributing resource cost to "the rate we
verified" rather than an average blurred across a whole oscillating test.
