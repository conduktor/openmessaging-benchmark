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

1. **Bracket** — starting at `rampStartRate` (default 10000), double the rate every
   `rampBracketPeriodSeconds` (default 3s) while backlog stays clean. The first breach sets a
   known-bad `hi`; the last clean rate is the known-good `lo`. (Handles the reverse case too — if
   even the start rate is already overloaded, it halves downward until it finds a clean `lo`.)
2. **Chop** — binary search the `[lo, hi]` bracket. Each candidate is held for `rampHoldSeconds`
   (default 30s), not just glanced at — a single 3s reactive snapshot (AIMD's approach) isn't
   enough to know a rate actually holds.
3. **Confirm** — once a candidate holds clean within `rampConvergenceTolerance` (default 5%), it
   isn't accepted immediately. It must pass `rampConfirmationHolds` (default 1) additional,
   consecutive clean holds at the *same* rate before being accepted. This is what makes "verified"
   mean something more than "passed once."
4. **Reopen on a failed confirmation** — if a confirmation hold fails (the rate looked fine, then
   didn't hold up), CHOP does not accept it anyway. It reopens the search: tightens `hi` to the
   failed rate, and falls back to the highest rate already *observed* to pass below it (never a
   blind guess — `lo` is always sourced from a real, recorded pass) as the new `lo`, then restarts
   the hold-and-confirm cycle from there.
5. **Non-monotonic flag** — every tested `(rate, passed/failed)` outcome is recorded. If a later
   verdict ever contradicts an earlier one (e.g. a lower rate fails after a higher one already
   passed), `isNonMonotonic()` is set and stays set for the rest of the run, even if the reopened
   search goes on to confirm cleanly. It's a signal that the system showed unstable behavior
   *somewhere* during discovery, so the final number may not reproduce as cleanly as a clean run
   would.
6. **Safety cap** — `rampMaxDiscoveryMinutes` (default 10) bounds total discovery time; if hit,
   discovery stops and reports the best confirmed-or-passed `lo` found so far.

### Configuration (workload YAML fields, all optional, only apply when `producerRate: 0`)

|           Field            |                Default                 |                                                                                                                                               Applies to                                                                                                                                               |
|----------------------------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rampAlgorithm`            | `AIMD`                                 | Selects the algorithm                                                                                                                                                                                                                                                                                  |
| `rampStartRate`            | 10000                                  | Both                                                                                                                                                                                                                                                                                                   |
| `rampPublishBacklogLimit`  | env `PUBLISH_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                   |
| `rampReceiveBacklogLimit`  | env `RECEIVE_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                   |
| `rampMaxBacklogSeconds`    | unset                                  | CHOP only — when set, replaces the two fields above with a limit that scales with the candidate rate (`limit = currentRate * rampMaxBacklogSeconds`), so the check is equally strict at every rate tried during bracket's exponential range instead of being loose at high rates and tight at low ones |
| `rampBracketPeriodSeconds` | 3                                      | CHOP only                                                                                                                                                                                                                                                                                              |
| `rampHoldSeconds`          | 30                                     | CHOP only                                                                                                                                                                                                                                                                                              |
| `rampConfirmationHolds`    | 1                                      | CHOP only                                                                                                                                                                                                                                                                                              |
| `rampConvergenceTolerance` | 0.05                                   | CHOP only                                                                                                                                                                                                                                                                                              |
| `rampMaxDiscoveryMinutes`  | 10                                     | CHOP only                                                                                                                                                                                                                                                                                              |

See `workloads/max-rate-chop-1-topic-100-partitions-100b.yaml` for a runnable example.

### Output

When discovery ends in a genuine confirm, the result JSON gets an extra `rampVerification` object
absent from AIMD runs and from CHOP runs that only hit the safety cap:

```json
"rampVerification": {
  "rate": 46000.0,
  "startEpochMillis": 1784817684100,
  "endEpochMillis": 1784817690849,
  "nonMonotonic": false
}
```

`startEpochMillis`/`endEpochMillis` bracket the *final* confirmation hold specifically — useful for
attributing resource usage (CPU/memory) to the verified rate rather than the whole warmup +
discovery + measurement run.

### Known limitation

Bracket phase deliberately overshoots (2x) past the real limit before backing off, to find `hi`.
That overshoot's fallout (backlog, GC pressure, broker load) isn't drained before the first chop
candidate starts its hold clock — so an early chop reading can still be affected by bracket's own
overshoot rather than reflecting steady state at that candidate alone. Not yet addressed; would
need an explicit drain/settle sub-phase between bracket and chop.

## Which to use

AIMD if you just want *a* number and don't care whether it's reproducible in a follow-up run at a
fixed rate. CHOP if you intend to take the discovered rate and actually rely on it — e.g. feeding
it into a fixed-`producerRate` workload later, or attributing resource cost to "the rate we
verified" rather than an average blurred across a whole oscillating test.
