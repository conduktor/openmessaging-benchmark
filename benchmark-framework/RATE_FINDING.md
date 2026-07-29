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
2. **Bracket** — starting at `rampStartRate`, double the rate every poll while backlog stays clean. A
   breach fails the candidate without waiting out the rest of its hold and sets a known-bad `hi`; a
   clean reading must hold for `rampBracketHoldSeconds` (default 20s) before being accepted as the
   known-good `lo`. Handles the reverse case too — if even the start rate is already overloaded, it
   halves downward until it finds a clean `lo`. Bounded at 20 doublings/halvings: if neither a `lo` nor
   a `hi` has been established by then, discovery gives up rather than searching indefinitely.

   **The bracket probe is deliberately much shorter than a chop hold, and that is safe.** Whenever it
   is shorter, the first thing chop does is re-run `lo` at *full* length before narrowing anything — the
   probe locates the bracket, the full hold certifies it. That matters because `lo` only ever moves
   upward, so an over-confirmed `lo` could otherwise never be undone. If the re-verification
   fails, `hi` becomes that `lo` and the search drops back to the highest rate history records as
   passing below it. A short probe disagreeing with a full hold does *not* set `isNonMonotonic()`:
   verdicts of different rigor are different measurements, not a contradiction. Measured on the AKS
   run's geometry (5,000 msg/s start, full-length chop holds, hard 589k ceiling): 90s probes cost 1,494s of
   discovery, 45s cost 1,134s (−24%), 20s cost 942s (−37%), 9s cost 846s (−43%) — all four converging
   on the identical rate.

3. **Chop** — binary search the `[lo, hi]` bracket. Each candidate is held for `rampHoldSeconds`
   (default 120s), not just glanced at — a single reactive snapshot (AIMD's approach) isn't enough
   to know a rate actually holds. The re-verification above is load-bearing rather than theoretical: on
   AKS a 20s probe passed 1,280,000 msg/s at a ratio of 0.999, and the full-length re-run of the same
   rate failed it at 0.883 — the search went on to confirm 1,153,675, so the probe had over-confirmed
   `lo` by 11%.

   The candidate after a *failed* hold is taken from the throughput that hold actually achieved rather
   than from the midpoint (`rampSeedFromAchievedRate`, on by default; set false to bisect blindly). A hold that
   asked for 800k msg/s and managed 600k has already measured the system; bisecting to 700k spends
   another full hold rediscovering that. The estimate is only used when it is informative and safe —
   at least `rampConvergenceTolerance` below the rate that just failed, and strictly above the
   highest rate already known to hold — otherwise the bracket is bisected as before. Both guards
   matter: a candidate that failed on *consumer* lag published everything it was asked for, so its
   achieved rate is the target restated rather than a measurement of capacity, and seeding from it
   would propose the rate that just failed.

   Do not expect the seed to save wall clock. It was originally justified on discovery time, but the
   convergence check described under **Confirm** shortens the same thing it does — a run of failing
   candidates — and largely absorbed the saving. Measured across seven capacities, seeding now wins two,
   loses three and ties two. What it still buys is placement: `lo` lands on a rate the system was
   measured at rather than on a midpoint.

4. **Recover** — after *every* failed candidate, run for up to `rampDrainSeconds` (default: the resolved
   `rampHoldSeconds`) at **half** the highest rate already known to hold, evaluating nothing and
   re-baselining the counters, exactly as **Settle** does for start-up transients. A failed candidate
   leaves the system carrying its overshoot, and judging the next one immediately measures that fallout
   rather than the candidate — which matters because `lo` only ever moves upward, so one contaminated
   reading cannot be undone.

   *Half* of `lo`, not `lo`: draining needs arrival below service, and `lo` means "keeps up", not "has
   spare capacity". It is a cap rather than a fixed wait, ending as soon as **both** sides have caught up
   — receive backlog inside its limit *and* publish delay back within tolerance, since work still queued
   in the producer client has not been published and so contributes no backlog at all. With nothing to
   drain it costs a single poll. Skipped entirely while bracket is still halving downward, because there
   is no known-good rate to drain at yet. See "Known limitation" for the measurements behind each of
   those three choices.

5. **Confirm** — chopping stops once the bracket is narrower than `rampConvergenceTolerance` (default
   5%), i.e. `(hi − lo) / lo ≤ tolerance`. That is checked on **both** routes into a narrower bracket:
   after a clean hold raises `lo`, and after a failed one tightens `hi`. Checking only the first is a
   trap — a stop condition reachable only when candidates succeed cannot stop a search whose
   candidates all fail, which is exactly the runaway case. A gateway encrypt arm ran eleven
   consecutive failures bisecting a bracket that had been inside the tolerance since the second one,
   spending 49% of the run's wall clock and eleven drain cycles to move the reported rate by 0.003%.
   When convergence is reached by failing, the search still routes through **Recover** first, since
   the candidate that just failed left a backlog and a confirmation hold that inherits one is judging
   the previous candidate's overshoot rather than its own rate.

   Reaching the tolerance isn't acceptance. The rate must then pass `rampConfirmationHolds`
   (default 1) additional, consecutive clean holds before being accepted. This is what makes "verified" mean something more
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

6. **Reopen on a failed confirmation** — if a confirmation hold fails (the rate looked fine, then didn't
   hold up), CHOP does not accept it anyway. It reopens the search: tightens `hi` to the failed rate and
   takes the highest rate already *recorded as passing* below it as the new `lo`, then restarts the
   hold-and-confirm cycle from there. The same fallback fires whenever any chop candidate at or below `lo`
   fails — including a failed re-verification of a cheap bracket probe — because `lo` and `hi` would
   otherwise collide and bisecting a zero-width bracket retests one rate forever.

   If history holds no passing rate below `hi` at all, `lo` falls back to `hi × 0.9`. That one *is* a
   blind guess, and it is reachable: a large `rampConvergenceTolerance` puts the confirmation rate below
   every rate previously recorded as passing. It only sets a starting point — the next hold judges it on
   its own merits.

7. **Non-monotonic flag** — every tested `(rate, passed/failed)` outcome is recorded, along with whether
   it came from a full-length hold. If a later verdict contradicts an earlier one (e.g. a lower rate fails
   after a higher one already passed), `isNonMonotonic()` is set and stays set for the rest of the run,
   even if the reopened search goes on to confirm cleanly. **Only full-length verdicts are compared:** a
   bracket probe run at a shorter `rampBracketHoldSeconds` is a cheaper measurement, not a weaker verdict
   on the same thing, so a probe disagreeing with a full hold is not a contradiction. When bracket and
   chop hold for the same length, every verdict is full-rigor and all of them are compared. It's a signal that the system showed unstable behavior
   *somewhere* during discovery, so the final number may not reproduce as cleanly as a clean run
   would.

8. **Safety cap** — `rampMaxDiscoveryMinutes` (default 45) bounds total discovery time; if hit,
   discovery stops and reports the best rate observed to pass so far — flagged, `WARN`-logged, and with
   no `rampVerification` attached, so a truncated search cannot be mistaken for a converged one. Bear in mind the
   settle period and every bracket/chop hold all draw from this same budget — with generous hold
   settings, raise this alongside them.

Two guards apply to every poll of every hold, before any of the above sees it:

- **A breach must persist.** `rampBreachPolls` (default 2) consecutive breaching polls are required
  before a candidate fails, and a hold that *ends* while still breaching fails regardless. One poll is
  one sample; on AKS a single dipping poll once capped a search 7% low on a hold whose own aggregate was
  99.13% of target, and nothing ever reopens `hi`.
- **A poll that acknowledges nothing is not evidence.** Every counter the finder sees is populated on
  ack, so an acknowledgement stall zeroes all of them at once while the messages are still in flight.
  Such a poll is not judged and the hold restarts rather than resuming a window it did not observe — a
  hold has to be a continuous observation to mean what it claims. If a stall never clears, the safety cap
  reports no `rampVerification`, which is the honest outcome for an outage.

### Verdict modes: we use `BACKLOG`

Each hold needs a clean/not-clean verdict before CHOP can act on it. Two predicates exist, selected by
`rampVerdict`. **We use `BACKLOG`, the default. `THROUGHPUT` is retained but not recommended** — the
reasoning is at the end of this section, and the evidence is in `docs/ramp-finder-trial-log.md`.

#### `BACKLOG` (default, and what we use)

A hold is clean while neither side falls behind. The two sides are judged differently, because they are
different kinds of quantity:

1. **Consumers — a level.** Cumulative `receiveBacklog`
   (`subscriptionsPerTopic × totalPublished − totalReceived`) must stay within the configured limit:
   `rampMaxBacklogSeconds` scaled by the candidate rate and clamped by `rampMaxBacklogFloor`/`Ceiling`,
   or the fixed `rampReceiveBacklogLimit` when the relative one is unset. Because backlog is a level,
   `rate × seconds` reads as "the consumers are at most this many seconds behind" — which is what the
   setting is meant to say.
2. **Producer — a flow.** `holdPublished ≥ rampMinThroughputRatio × holdExpected`, accumulated over the
   hold so far. A shortfall is a rate, not a depth, so comparing one poll's shortfall against a message
   count said something else entirely: at a 1-second poll, `rampMaxBacklogSeconds: 0.5` permitted a 50%
   shortfall *every poll, indefinitely*, because nothing accumulated between polls. As a fraction it
   needs no per-rate tuning, fast-fails a gross shortfall on the first poll, and ignores a small one.
   (`rampPublishBacklogLimit` is therefore unused by CHOP; it remains only for AIMD.)

Two polls must breach consecutively before a candidate fails (`rampBreachPolls`, default 2), and a poll
that acknowledges *nothing* is treated as an absence of data rather than a breach — see "When discovery
refuses to answer" below for both.

#### `THROUGHPUT` (retained, not recommended)

A scale-free alternative. Per hold, all three of:

1. `published ≥ ratio · expected` — the producer kept up with the target rate.
2. `received ≥ ratio · subscriptionsPerTopic · published` — the consumers drained what was published.
   This is a check on *divergence*, not depth: if the receive backlog grew by `ΔB` while the hold
   published `P`, the consumers moved `subscriptions·P − ΔB`, so it is exactly
   `ΔB ≤ (1 − ratio)·subscriptions·P`.
3. `secondHalfRate ≥ ratio · firstHalfRate` — throughput did not *decline* across the hold.

`ratio` is `rampMinThroughputRatio` (default 0.95) throughout, and there is no per-poll fast-fail: the
verdict is decided once, at hold completion.

#### Why we chose `BACKLOG`

`THROUGHPUT` was added for a good reason. A fixed message count cannot be right at two different rates —
a healthy pipeline's in-flight depth scales with throughput — and a large *stable* backlog is healthy
even though a count-based check rejects it. That diagnosis was correct, and `BACKLOG` has since absorbed
the fix: its limit scales with rate, and its producer side is now a ratio, which is `THROUGHPUT`'s own
producer gate. The two are no longer far apart.

What separates them is **what each can see**. Every counter the finder receives is populated when a
message is *acknowledged*, so work that is in flight and unacked is invisible. `BACKLOG` watches a
level, which starts rising the moment arrival exceeds service. `THROUGHPUT` watches flows and ratios,
which stay healthy for as long as something downstream can absorb the overshoot — and only droop once it
cannot. **Backlog is a leading indicator of saturation; achieved rate is a lagging one.** Check 3 above
narrows that gap but cannot close it: a hold whose buffers absorb for its entire length is flat in both
halves, and no statistic computed inside that hold can distinguish it from a healthy one.

That is not a theoretical preference. Measured on AKS across three transport arms, one job, same
cluster:

|     arm     |   verdict    | confirmed |    avg publish delay over a 10-minute window     |
|-------------|--------------|----------:|--------------------------------------------------|
| direct      | `BACKLOG`    | 1,111,681 | 1,016 ms (a mid-window excursion that recovered) |
| transparent | `BACKLOG`    | 1,177,872 | **4.4 ms**                                       |
| encrypt     | `BACKLOG`    |   119,670 | **0.0 ms**                                       |
| direct      | `THROUGHPUT` | 1,099,507 | 16.6 ms                                          |
| transparent | `THROUGHPUT` | 1,421,882 | **22,930 ms**, climbing to 55,585 ms             |
| encrypt     | `THROUGHPUT` |   128,969 | 0.0 ms                                           |

`THROUGHPUT` confirmed 1,421,882 msg/s on the transparent arm — 21% above what `BACKLOG` found on the
same arm — and the measurement window that followed delivered 10% *less* than that while publish delay
climbed monotonically to 55 seconds. Every one of `THROUGHPUT`'s three checks passed, and
`nonMonotonic` was false, because the gateway in that path had enough buffer to keep the ratios looking
healthy. Its error tracks **how much buffering sits between producer and broker**, which is why it
appears on the gateway arms and not on direct.

So: `BACKLOG` for anything whose number we intend to rely on. `THROUGHPUT` remains for the one shape it
genuinely handles better — a topology carrying a large, *stable* standing backlog that the consumers
keep pace with but never close, where a level-based check has no correct threshold. If you use it,
check publish delay in the measurement window before believing the rate.

### Configuration (workload YAML fields, all optional, only apply when `producerRate: 0`)

**Every default here comes from the AKS trial data rather than from caution.** The ones that matter:

- `rampMaxBacklogSeconds: 0.5` — a rate-scaled limit. The old fixed-count fallback was 0.9 *milliseconds*
  of tolerance at 1.1M msg/s and capped the answer ~5x low above 200k msg/s.
- `rampBracketHoldSeconds: 20` — bracket only locates the knee; `lo` is re-verified at full length before
  chop narrows anything. Worth −37% discovery against a 90s probe, converging on the identical rate.
- `rampSeedFromAchievedRate: true` — −12%, no measured accuracy cost.
- `rampHoldSeconds: 120` — see "Calibrating `rampHoldSeconds`" below. This is the setting to change if
  you change one; the rest travel well between clusters and this one does not.

On these settings three arms took 16–22 minutes of discovery each, of which 75–85% is chop's
full-length holds. Bracket is down to ~3 minutes, so there is nothing left to win there: the remaining
levers are `rampHoldSeconds` and `rampConvergenceTolerance`, and both trade accuracy for time.

|           Field            |                Default                 |                                                                                                                                                                                                                                                                    Applies to                                                                                                                                                                                                                                                                     |
|----------------------------|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rampAlgorithm`            | `AIMD`                                 | Selects the algorithm                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `rampVerdict`              | `BACKLOG`                              | CHOP only — selects the clean/not-clean predicate for holds; `THROUGHPUT` is the scale-free alternative (see "Verdict modes" above). AIMD never builds a `RampRateFinder`, so this field has no effect on it                                                                                                                                                                                                                                                                                                                                      |
| `rampMinThroughputRatio`   | 0.95                                   | CHOP only — THROUGHPUT verdict                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rampStartRate`            | 10000                                  | Both                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `rampPublishBacklogLimit`  | env `PUBLISH_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `rampReceiveBacklogLimit`  | env `RECEIVE_BACKLOG_LIMIT`, else 1000 | Both                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `rampMaxBacklogSeconds`    | 0.5                                    | CHOP only — when set, replaces the two fields above with a limit that scales with the candidate rate (`limit = clamp(currentRate * rampMaxBacklogSeconds, rampMaxBacklogFloor, rampMaxBacklogCeiling)`), so the check is equally strict at every rate tried during bracket's exponential range instead of being loose at high rates and tight at low ones                                                                                                                                                                                         |
| `rampMaxBacklogFloor`      | 1000                                   | CHOP only — hard floor (messages) on the limit `rampMaxBacklogSeconds` computes, matching the old fixed-count default so the relative check can never become stricter than a fixed-count check would have been at any rate; only meaningful when `rampMaxBacklogSeconds` is set (see the trial log)                                                                                                                                                                                                                                               |
| `rampMaxBacklogCeiling`    | 500000                                 | CHOP only — hard cap (messages) on the same limit; only meaningful when `rampMaxBacklogSeconds` is set (see the trial log for why this exists)                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rampBracketPeriodSeconds` | 3                                      | CHOP only — poll cadence, not a hold duration                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `rampSettleSeconds`        | 30                                     | CHOP only — grace period at start before backlog counts at all                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rampBracketHoldSeconds`   | 20 (or `rampHoldSeconds` if shorter)   | CHOP only — how long a bracket candidate must hold clean; shorten independently once you trust bracket's coarser candidates need less scrutiny                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rampDrainSeconds`         | resolved `rampHoldSeconds`             | CHOP only — cap on the recovery period run after a *failed* candidate, at **half** the highest rate already known to hold, so there is headroom for the queue to actually shrink. Ends as soon as the backlog is back within the limit it is judged against, so it costs one poll when there is nothing to drain. Set 0 to disable. Skipped while bracket is still halving downward, since there is no known-good rate to drain at yet. Requires *both* sides caught up: receive backlog inside its limit and publish delay back within tolerance |
| `rampHoldSeconds`          | 120                                    | CHOP only — how long a chop candidate must hold clean. The most consequential ramp setting: a hold shorter than the broker's burst-absorption time at the candidate rate accepts a rate the measurement window then fails on, and no predicate can detect that from inside the hold. Size it from the cluster's cache depth, not from this default                                                                                                                                                                                                |
| `rampConfirmationHolds`    | 1                                      | CHOP only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `rampBreachPolls`          | 2                                      | CHOP only, `BACKLOG` verdict — consecutive polls that must breach before a candidate fails. 1 restores the old one-sample behaviour; on AKS a single dipping poll capped a search 7% low on a hold whose aggregate was 99.13% of target, and nothing ever reopens `hi`                                                                                                                                                                                                                                                                            |
| `rampConvergenceTolerance` | 0.05                                   | CHOP only                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `rampSeedFromAchievedRate` | true                                   | CHOP only — after a failed hold, take the next candidate from the throughput that hold achieved instead of bisecting. Measured at ~12% less discovery time on a 5000 → 841k geometry; no effect when the start rate is already above capacity, since the bracket's downward halving path is not seeded                                                                                                                                                                                                                                            |
| `rampMaxDiscoveryMinutes`  | 45                                     | CHOP only — coupled to `rampHoldSeconds`: budget roughly `settle + 15 x rampHoldSeconds`, plus the drain if set. Hitting the cap reports the best rate that held without confirming it, with a WARN and no `rampVerification`                                                                                                                                                                                                                                                                                                                     |

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

A single figure rather than a band is a deliberate choice, and it survives being checked: across two
independent AKS runs on the same cluster the gateway-passthrough arm confirmed 1,177,872 then 1,177,897
msg/s, a spread of 0.002%, with the other two arms inside 4% and 8%. What reproduces less well is whether
a *given* window stays clean — the same arm showed a mid-window publish-delay excursion in one run and
nothing comparable in the next at the same rate. So treat the rate as reproducible and the measurement
window as the independent check on it, not as a formality.

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
silently, so a misspelled one otherwise looks like it applied), then two `INFO` series.

One `FINDER-HOLD` line per completed hold — the *verdict* record:

```
FINDER-HOLD phase=CHOP rate=4750.0 verdict=exceeded confirming=false expected=4750 published=4500
            received=4500 backlog=310 backlogPeak=1180 achievedRatio=0.947 drainRatio=1.000
            trendRatio=0.923 bracket=[4500.0, 5000.0]
```

`achievedRatio` is `published / expected`, `drainRatio` is
`received / (subscriptionsPerTopic x published)`, and `trendRatio` is the hold's second half over its
first — the three quantities the `THROUGHPUT` verdict compares against `rampMinThroughputRatio`, shown
in both modes. `trendRatio` reads `n/a` when the hold was too short to split, which under `BACKLOG` is
every failing hold, since those fast-fail on their first breaching poll. `backlog` is the level at the
deciding poll and `backlogPeak` the deepest the hold reached; they differ when a hold recovers after
falling behind, which is what a knee looks like from the outside.

One `FINDER-POLL` line per poll — the *diagnostic* series, for plotting:

```
FINDER-POLL t=124 rate=640000 achieved=639871 backlog=241 delayP50Ms=0.1 delayP99Ms=1.2
            delayMaxMs=3.4 latencyP99Ms=8.1
```

This exists because the verdict record is not enough to locate a knee. It is at hold granularity (120s
by default), and it carries no latency at all, because the finder is handed counters and
never sees any. Backlog and publish delay both start moving well before a hold's *aggregate* verdict
flips — which is precisely how a hold can accept a rate the measurement window then fails on. Plot
`rate`, `achieved`, `backlog` and `delayP99Ms` on one time axis and the knee is visible directly.

Both series come from a single `getPeriodStats()` call per poll, so the counters and the latency
describe the same instant with no skew between them.

Three lines appear only when something happens, and each is worth grepping for:

```
FINDER-SEED  next candidate 131332 msg/s from the failed hold's achieved rate
             (bisecting [80000.0, 160000.0] would have tried 120000.0)
FINDER-DRAIN recovery complete after 6s at 640000.0 msg/s (backlog 8386, delayP99 0ms,
             consumerCaughtUp=true producerCaughtUp=true); resuming at 1280000.0 msg/s
FINDER-STALL no acknowledgements in a 3000ms poll at 10000.0 msg/s (expected 30000);
             not judged, hold restarted
```

- **`FINDER-SEED`** — the search jumped to a failed hold's measured throughput instead of bisecting. It
  prints the midpoint it skipped, so the saving is visible.
- **`FINDER-DRAIN`** — `complete` versus `capped` tells you whether recovery finished or gave up, and
  `consumerCaughtUp`/`producerCaughtUp` which side was not ready. Frequent `capped` means the failed
  candidates are leaving more behind than `rampDrainSeconds` allows for.
- **`FINDER-STALL`** — a poll in which *nothing* was acknowledged. Every counter the finder sees is
  populated on ack, so an acknowledgement stall zeroes all of them at once while the messages are still
  in flight; that is an absence of data rather than a breach, so the candidate is not judged and the hold
  restarts. On AKS a pair of these once failed a rate that had just held cleanly and drove the search
  ~120x low. A handful is normal; a steady stream means the transport is stalling, not that the rate is
  wrong.

### Calibrating `rampHoldSeconds`

This is the one setting that cannot be derived up front, because it depends on how long *your* broker can
absorb an oversubscribed rate — a property of its page cache, batching and socket buffers, not of the
rate. Two things bound it:

- It must **outlast absorption**. Until buffers saturate the broker acks at the full target rate, so a
  shorter hold accepts a rate the measurement window then fails on.
- `rampMinThroughputRatio` caps the **detectable overshoot at about 5%**. A candidate 3% over capacity
  asymptotes to a ratio of ~0.97 and never crosses 0.95, at any hold length. Precision comes from the
  ratio and `rampConvergenceTolerance`, not from holding longer.

Past roughly twice the absorption time, a longer hold buys nothing. So calibrate it once, from a run's
own `FINDER-POLL` series, rather than guessing:

1. Run once with a generous hold (180s is a good probe value).
2. For each hold that **passed**, group its `FINDER-POLL` lines and compute the running cumulative ratio
   `Σ achieved / Σ rate` as the hold progresses.
3. Find where that ratio stops moving. Set `rampHoldSeconds` to about **twice** that.

Worked example, from the AKS run this default came from. Across 18 full-length passing holds, 11 sat
within 0.01 of their final ratio from 30s onward — 150 seconds each producing no new information. One
hold showed the absorption signature clearly:

```
rate=1,205,023   30s=1.000   60s=0.967   90s=0.962   120s=0.962   150s=0.957   180s=0.955
```

Settled within 0.012 of its final value by 60s. Hence the 120s default: twice the point at which every
verdict was determined, with the slow tail still inside it. **At 30s that same hold read a confident
1.000 against a true 0.955** — which is why 30s is not safe, and why a wrongly-passed `lo` matters: it
only ever moves upward.

### Known limitation

Bracket phase deliberately overshoots (2x) past the real limit before backing off, to find `hi`.
That overshoot's fallout (backlog, GC pressure, broker load) is not drained before the next
candidate starts its hold clock — so a reading can be affected by the previous candidate's overshoot
rather than reflecting steady state at the candidate under test. This matters more than it sounds:
the bracket phase only ever moves `lo` *upward*, so a single contaminated reading cannot be recovered
from later.

**Handled by `rampDrainSeconds`** (on by default, at the resolved `rampHoldSeconds`; set 0 to disable).
A failed candidate is followed by a recovery period during which nothing is evaluated and the counters
are re-baselined, exactly as `rampSettleSeconds` does for start-up transients. Three details matter:

- **It runs at half of `lo`, not at `lo`.** Draining needs arrival below service. `lo` means "keeps up",
  not "has spare capacity", so at `lo` the queue shrinks at `capacity − lo` — nearly nothing. Two AKS
  recoveries ran their full 180-second cap and gave up with the producer still 10 and 24 seconds behind,
  draining at ~1.16M against a ~1.17M ceiling. Half gives real headroom; since nothing is measured during
  recovery, running slower costs nothing.
- **Both sides have to catch up.** Receive backlog inside its limit *and* publish delay back within
  tolerance. Work sitting in the producer client's buffer has not been published, so it contributes no
  receive backlog at all — recovery previously declared itself complete in two polls while the producer
  was seconds behind its own schedule. `FINDER-DRAIN` reports `consumerCaughtUp` and `producerCaughtUp`
  separately so a capped recovery says which side was not ready.
- **It is a cap, not a fixed wait**, so it ends as soon as both sides are clear — a single poll when
  there is nothing to drain. And it is skipped entirely while bracket is still halving downward, because
  there is no known-good rate to drain at yet.

Note what this does and does not fix. It removes *cross-candidate* contamination. It does not make a
history-dependent verdict into a function of rate — see the trial log, where the same 800k candidate
returned opposite verdicts on the same broker minutes apart.

**Expect enabling it to raise the reported rate, not lower it.** Counterintuitive, and worth
understanding before reading a before/after pair as a regression. On an absorbing broker, leftover
contamination makes every candidate fail more readily, which pushes the accepted rate down — and on a
model whose buffer never refills, that accidentally lands very close to the true ceiling. Draining
gives each candidate a fair hold, which is correct, and the accepted rate rises accordingly. The
contamination was not producing a good answer for a good reason; it was masking the fact that a
5%-over-a-finite-hold predicate is too permissive when a broker can absorb the overshoot. Fixing the
isolation exposes that. Hold length is the lever that addresses it.

### The measurement window's first interval

`printAndCollectStats` derives each interval's rate as `periodStats.messagesSent / (now − previousPoll)`,
and `messagesSent` accumulates until something calls `toPeriodStats()`. Nothing did so between
`startLoad()` and the window's first poll, so that first interval used to absorb everything published
beforehand — on a `producerRate: 0` run, **15,439,670 msg/s** against a steady-state **481,000**, which
is roughly six minutes of discovery folded into one 10-second sample.

**Fixed:** `LocalWorker.resetStats()` now clears the per-period counters as well as the latency
recorders, so the window starts from zero. Deliberately *not* via `WorkerStats.reset()`, which also
clears `totalMessagesSent`/`totalMessagesReceived` — `buildAndDrainBacklog` is launched before
`resetStats()` and runs concurrently for the whole test, computing its remaining backlog from those
totals, so clearing them mid-flight would collapse its backlog to zero and let it conclude the drain had
finished. `WorkerStatsTest` pins both halves of that.

Worth knowing anyway, for two reasons. Any run from before this fix has a junk first sample in
`publishRate`/`consumeRate` and the byte-rate equivalents, so a mean over those lists inherits it. And a
single anomalous interval can still arise for ordinary reasons — a GC pause, a leader election — so
prefer the median over the mean when you want "the rate this window sustained". Latency figures were
never affected: `resetLatencies()` always cleared both the interval and cumulative recorders.

### Resource-bound paths: a confirmed rate can sit at the edge of a hard resource limit

CHOP verifies a candidate from the producer's and consumer's own counters. It cannot see the resource
that is actually the bottleneck -- e.g. the gateway CPU on an encrypting proxy path. When that resource
is the wall, a rate can be confirmed cleanly and still fail to reproduce.

A gateway campaign made this concrete. CHOP confirmed **129,326 msg/s** for an encrypting arm; a later
fixed-rate confirmation at that exact target averaged **~96,000** (75%) and plateaued at **~104,000** (80%)
after a ~50-second near-total stall, with the gateway pegged at its 2-core limit and p99 publish latency at
46 s. It was not a measurement artifact: during discovery
the rate held for ~7.5 minutes with backlog of 1--2 K messages and publish delay of ~31 microseconds --
clean by every producer/consumer signal, and a *600-second* measurement window at the same rate immediately
afterwards sustained 100.0% of it. The difference was the gateway CPU, which CHOP never sees. Measured over
the confirming hold itself: **median 1.690 of 2.0 cores (84.5%), peak 1.807 (90.3%)**, already throttling
at ~1.9 CFS periods/s. In the failed confirmation the same gateway sat at a median of **1.998 (99.9%)** and
was throttled in essentially every period (~10/s) -- from its very first sample, before any back-pressure
existed. Encryption here is per-*message*-bound (2 pegged cores move only ~9.6 MB/s of 100-byte messages),
so 129k sits close enough to the CPU budget that a **≥18%** change in per-message cost consumes the whole
margin. Brokers, workers and arm ordering were ruled out -- a non-gateway arm on the same clusters was
unaffected. Node hardware was *not* ruled out: the two runs are different physical clusters with the same
pool config and VM family, and this is the hypothesis the data most supports.

No producer/consumer-side gate can catch this -- not the backlog check, not the `THROUGHPUT` ratio, not a
latency gate -- because at ramp time the wall is *approached, not breached*: throughput, backlog, and
delay are all genuinely clean at ~85% CPU. The only signal that reveals the fragility is the bottleneck
resource's own utilization.

**Direction (not yet implemented): a resource-headroom gate.** Reject or derate a candidate whose
bottleneck-resource utilization during the confirming hold exceeds a headroom threshold. It can be applied
harness-side with no finder change -- `rampVerification` already carries the confirmation window, and the
harness already samples resource metrics over exactly that window (`chop-verify.metrics.json`).

Two things to get right, both learned by checking a proposed threshold against the run it was meant to
reject:

- **Gate on peak (or a high percentile), not the median.** Over the confirming hold above, the median is
  84.5% -- so a "reject above 85%" gate *passes* the very rate that failed to reproduce. Only the peak
  (90.3%) rejects it.
- **85% is too high.** Pricing candidates against that run's measured ~76,000 msg/s per gateway core, the
  rate that actually held on the second cluster (~104k, its post-stall plateau) sits at **~68%** of the
  limit; an 85%-derated target (~120k, i.e. 78%) would still have failed. Express the threshold as
  **required margin ≥ measured node-to-node variance** rather than as a fixed number -- a fixed 85% encodes
  an assumption of ~15% variance that this campaign refutes. Note this cannot be calibrated from a single
  cluster: you need the same arm on two cluster instances to know the variance at all.
- **Read utilization only over an otherwise-clean hold.** Past the knee the signal inverts: in the failed
  confirmation the gateway's CPU *fell* to 0.27 cores during a ~50-second stall, because the producer had
  wedged on `buffer.memory` and stopped feeding it. Low utilization there means starvation, not headroom.

This is complementary to the verdict choice: the verdict decides what "kept up" means; the headroom gate
refuses a rate that kept up only by consuming the last of a hard resource. Full design in
[`docs/superpowers/specs/2026-07-24-chop-throughput-verdict-design.md`](../docs/superpowers/specs/2026-07-24-chop-throughput-verdict-design.md);
the supporting runs are Findings 18--19 in
[`docs/ramp-finder-trial-log.md`](../docs/ramp-finder-trial-log.md).

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

With CHOP, leave `rampVerdict` at its `BACKLOG` default. See "Verdict modes" above for why, and
`docs/ramp-finder-trial-log.md` for the runs behind it.

Whichever you use, the number is only as good as the hold it was verified over. `rampHoldSeconds` has to
exceed the time your broker can absorb an oversubscribed rate — a property of its cache, not something
the finder can discover a priori. The 120s default suits the cluster it was measured on; see
"Calibrating `rampHoldSeconds`" for how to derive it for yours from one run rather than guessing.

And whatever it says, **check publish delay in the measurement window that follows**. That single check
has caught every over-confirm in the trial log, including ones where every gate the finder applied
reported clean and `isNonMonotonic()` was false.
