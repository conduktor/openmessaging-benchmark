# Ramp rate-finder trial log

Working notes, not user documentation. For how to configure and read a rate-finding run, see
`benchmark-framework/RATE_FINDING.md`.

This is the evidence trail behind the rate finder's design: what was run, what it produced, what was
concluded, and where a conclusion was later corrected or left unresolved. It is kept because several
of the finder's less obvious guards exist only because of a specific observed failure, and without
the observation they look arbitrary and invite removal.

Entries are chronological and preserve their original wording, including hypotheses that were never
confirmed and at least one correction. Treat any diagnosis here as the reasoning at the time rather
than as settled fact.

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

### Trial finding: with no floor, the same tolerance can also spiral *down* to a trivial rate

Found re-running the same harness integration test, 2026-07-24, same day. **Correction**: this
was originally reported as confirmed running the `rampMaxBacklogCeiling` fix (`7f97593`) based on
CI-completion timing alone. On closer inspection that's unconfirmed either way: the image was
published to the registry 26 seconds before this run's first cell (`aimd`, sharing the same
dedicated worker nodes `chop` later reused) had already logged "Starting benchmark" — too tight a
window for a fresh multi-hundred-MB pull to complete, and `imagePullPolicy: IfNotPresent` means
whatever `aimd` cached first is what `chop` would have reused regardless of what the registry tag
pointed to by then. Genuinely unknown which version ran here. Doesn't change the finding below,
though — the missing floor is a gap in the rate-relative computation itself, present with or
without the ceiling fix.

**Setup**: same as above, but `rampMaxBacklogSeconds` tightened from `1.0` to `0.1` (in direct
response to the previous finding) — everything else unchanged (`rampStartRate: 20000`,
`rampBracketHoldSeconds: 30`, `rampHoldSeconds: 60`, `rampConvergenceTolerance: 0.05`,
`rampMaxDiscoveryMinutes: 12`, `testDurationMinutes: 15`).

**Result**: CHOP reported a genuine confirm — `rampVerification.rate = 0.9155273437500011`,
`nonMonotonic = false`. Discovery took ~7m39s (well inside budget), and the final confirmation
hold was a clean, uncontested 60s (`2026-07-24T10:53:59Z -> 2026-07-24T10:54:59Z`). The
measurement phase then ran at that ~1 msg/s rate for the full 15 minutes with **zero backlog and
~2.5ms latency throughout** — i.e. the system was never remotely stressed at this rate. AIMD (same
run, same topology) had no trouble sustaining tens of thousands of msg/s. A ~1 msg/s "sustainable
rate" for a 1-topic/100-partition Kafka cluster is not a real capacity limit; something in
discovery talked itself down to a trivial number and then confirmed it cleanly, with no
contradiction to flag.

**Diagnosis (hypothesis, not confirmed against the source)**: `rampMaxBacklogSeconds` scales the
tolerance down along with the candidate rate just as much as it scales it up — there's a ceiling
now, but no floor. If bracket's first candidate (or an early one) trips a false failure for any
reason (a moment of real jitter, a coincidental timing artifact — hard to say without instrumenting
the actual hold windows), the halving path that follows makes the *next* candidate's tolerance
*smaller* in direct proportion to the *lower* rate being tested. That's the wrong direction for a
recovery mechanism: each step down make the check stricter, not more lenient, so a single early
false-positive can cascade all the way to a floor near zero rather than settling once real headroom
appears. The old fixed-count default (1000 messages, rate-independent) didn't have this failure
mode — it got *easier* to pass as the candidate rate fell, providing a natural backstop.

**Suggested follow-up**: give `rampMaxBacklogSeconds` a floor symmetric to the new ceiling, e.g.
`limit = clamp(currentRate * rampMaxBacklogSeconds, rampMaxBacklogFloor, rampMaxBacklogCeiling)`,
with a sensible default floor (perhaps the old fixed-count default, 1000) so the relative check
can't become stricter than a fixed-count check would have been at any rate, however low the search
has already fallen.

Harness-side note (not a CHOP issue, same as above): `chop-verify.metrics.json` was produced again
alongside the whole-job capture, confirming the resource-sampling integration is unaffected by
which rate CHOP happens to land on — it faithfully samples whatever window `rampVerification`
reports, correct data or not.

**Reproduced, same day**: reran the identical setup on a *freshly provisioned* cluster (new node
pools, so no `imagePullPolicy: IfNotPresent` caching ambiguity this time — this run unambiguously
pulled whatever the `pr-19` tag currently resolves to). Result: `rampVerification.rate =
0.9155273438210543` — matching the original run's `0.9155273437500011` to **7 significant
figures**, `nonMonotonic: false` again, identical flatline-at-1-msg/s measurement pattern. This
isn't environmental flakiness; it's a deterministic outcome of the same starting conditions
(`rampStartRate: 20000`, `rampMaxBacklogSeconds: 0.1`) hitting the same missing-floor gap every
time. Confirms the diagnosis above rather than just being a one-off fluke.

**Fixed**: `rampMaxBacklogFloor` (default 1000, matching the old fixed-count default per the
suggested follow-up above) now clamps the same limit from below, symmetric to
`rampMaxBacklogCeiling`. The relative check can no longer become stricter than a fixed-count check
would have been at any rate, however far the search has already fallen.

### Trial finding: `BACKLOG` under-reported the true sustainable rate by up to ~320x on a healthy cluster

Found re-running the same harness integration test (`conduktor/benchmarks`, workflow run
`30103329337`), 2026-07-24, same AKS `representative` preset, same topology (1 topic / 100
partitions / 100-byte messages, direct-to-Kafka, single producer/consumer) as the trial findings
above. This is the finding that motivated adding `rampVerdict: THROUGHPUT`.

**Setup**: three loads on the same cluster for comparison — `rampAlgorithm: AIMD` (default,
`BACKLOG` verdict, no alternative available), `rampAlgorithm: CHOP` with
`rampMaxBacklogSeconds: 0.1` (`BACKLOG` verdict), and `rampAlgorithm: CHOP` with
`rampMaxBacklogSeconds: 1.0` (`BACKLOG` verdict).

**Result**: both `BACKLOG`-based runs reported low numbers relative to what the cluster could
actually sustain — AIMD peaked at ~13,500 msg/s, and CHOP with `rampMaxBacklogSeconds: 0.1`
"confirmed" 2,734 msg/s. The `rampMaxBacklogSeconds: 1.0` run, by contrast, discovered 880,000
msg/s and that rate then held cleanly for the *entire* 15-minute measurement window — publish and
consume rates both tracking ~880k, backlog bounded (max ~16,432 messages, not growing over the
window), average publish-delay 0.12ms. That's the true sustainable rate for this cluster; the
other two runs under-reported it by roughly 65x (AIMD) and 320x (`chop-floor`) respectively.

**Diagnosis**: same root cause as the two trial findings above, just observed on a cluster healthy
enough to make the scale mismatch obvious rather than merely wrong. A fixed-ish backlog-count
limit is calibrated for *some* rate; at 880k msg/s, thousands of messages of in-flight backlog is
normal pipeline depth, not a problem — but a `BACKLOG`-style check tuned tight enough to be
meaningful at low rates will flag that depth as a breach and cap discovery far below what the
system can actually do. `rampMaxBacklogSeconds: 1.0` happened to be loose enough to let the search
reach the real ceiling here; `0.1` was not.

**Fixed**: added the opt-in `rampVerdict: THROUGHPUT` mode (see "Verdict modes" above), which
checks published/received ratios against the target rate instead of an absolute or rate-scaled
backlog count, so the same threshold is neither too strict nor too loose regardless of what rate
is being tested. `BACKLOG` remains the default — `THROUGHPUT` is opt-in until it has more runs
behind it.

### Trial finding: THROUGHPUT over-reports ~1.7x vs BACKLOG, and longer holds do not fix it

Superseded the entry below, on 2026-07-27, with `rampHoldSeconds` tripled from 15s to 45s (bracket
held to the same duration) and `rampDrainSeconds: 45` enabled. Same single-broker Testcontainers
Kafka, `rampStartRate: 5000`, 2-minute measurement window.

|   Verdict    | Holds | Confirmed | Window achieved | Avg publish delay | Max backlog |
|--------------|-------|-----------|-----------------|-------------------|-------------|
| `BACKLOG`    | 45s   | 841,158   | 841,088         | **0.9 ms**        | 154         |
| `BACKLOG`    | 45s   | withheld  | 760,584         | 0.1 ms            | 1,199       |
| `THROUGHPUT` | 45s   | withheld  | 1,421,630       | **2,612 ms**      | 54,622      |

**The earlier conclusion was wrong, and wrong for a specific reason: it compared across verdict
modes.** It set `THROUGHPUT` at a 15s hold (3,037 ms delay) against `BACKLOG` at a 45s hold (0.9 ms)
and attributed the difference to hold length. Holding the verdict constant instead:

- `THROUGHPUT`, 15s hold → 3,037 ms delay. `THROUGHPUT`, 45s hold → 2,612 ms delay. Tripling the
  hold barely moved it.
- `BACKLOG` at 45s lands on 841k and the window sustains it with sub-millisecond delay.

So hold length is not the dominant error. `THROUGHPUT` selects ~1.7x `BACKLOG`'s rate, and that rate
is not sustainable: 2.6 seconds of average publish delay and a 54,622-message backlog over the
window.

**Diagnosis: the achieved-rate ratio is a lagging indicator of saturation; a backlog count is a
leading one.** At 1.42M the broker really does ack 1.42M/s for the whole 45-second hold, absorbing
the excess into page cache and socket/producer buffers — so `published/expected` reads ~1.0 and the
consumer keeps pace with what was published, and both `THROUGHPUT` gates pass honestly. Throughput
only droops once absorption is exhausted, which on this hardware takes longer than the hold *and*
longer than the measurement window. A backlog-count check sees the queue depth rise immediately,
which is why `BACKLOG` gets the right answer here.

This partially vindicates the predicate the `THROUGHPUT` design set out to replace. The count-based
check is genuinely wrong about *scale* — that is what the ~320x under-report below demonstrates — but
it is watching the right variable: queue growth, which leads, rather than achieved throughput, which
lags.

**Suggested direction** (not implemented): keep the scale-free ratio for what it is good at, and add
a trend gate for what it is not — require that the backlog is not *growing* across the hold, or that
achieved throughput in the hold's second half matches its first. Both detect a filling buffer at any
hold length, which no aggregate-over-the-whole-hold statistic can. Note also that 3 of 5 real runs
here withheld their answer to `isNonMonotonic()`, so neither mode currently produces a reliable
point value on this broker.

### Superseded: hold length, not the verdict predicate, is the dominant error

Found running the strengthened `ChopRateFinderKafkaIT` against a single-broker Testcontainers Kafka
on 2026-07-27, after the confirm-below-the-knee and achieved-rate-reporting changes. Three discovery
runs on the same broker, identical config apart from `rampVerdict` (1 topic / 10 partitions /
100-byte messages, `rampSettleSeconds: 5`, `rampBracketHoldSeconds: 10`, `rampHoldSeconds: 15`,
`testDurationMinutes: 1`):

| Run |   Verdict    | Confirmed target | Reported (achieved) | Measurement-window publish delay |
|-----|--------------|------------------|---------------------|----------------------------------|
| 1   | `BACKLOG`    | withheld         | none                | 0.7 ms                           |
| 2   | `BACKLOG`    | 1,292,594        | 1,291,997           | 37.8 ms                          |
| 3   | `THROUGHPUT` | 1,406,000        | 1,380,771           | **3,037 ms**                     |

Three things to take from this:

- **Achieved-rate reporting works.** Reported vs confirmed target agree to 0.05% (run 2) and 1.8%
  (run 3), so `rampVerification.rate` is now a number the confirmation hold really delivered.
- **`isNonMonotonic()` fires on real hardware, and still costs the whole answer.** Run 1 found
  ~984k, contradicted itself during the search, and reported nothing. Confirming below the knee
  removes the *coin-flip-at-the-confirm* source of this, but not contradictions arising earlier in
  the search. Runs 1 and 2 differ only in luck, so a `BACKLOG` run's success here is not
  reproducible.
- **A confirmed, achieved rate can still be unsustainable.** Run 3 held 1.38M for a 15-second hold
  at `achievedRatio` 0.982, then averaged **3 seconds** of publish delay over the following
  60-second measurement window. The producer was not keeping up at all; the hold was simply shorter
  than the broker's burst-absorption time at that rate. This is the hold-length sensitivity the
  `THROUGHPUT` design explicitly listed as out of scope, and with the scale and count problems now
  fixed it is the largest remaining source of error — not the verdict predicate.

**Open, not fixed.** `rampHoldSeconds` has no principled default: it must exceed the broker's
burst-absorption time at the candidate rate, which is itself unknown before the measurement. Two
directions worth considering, neither implemented: scale the hold with the candidate rate (absorption
time is roughly buffer-depth / overshoot, so higher rates need longer, not equal, holds); or judge a
hold on whether achieved throughput is *flat across its own second half* rather than on its aggregate
ratio, which detects a filling buffer regardless of hold length. Until then, treat a discovered rate
as unverified unless `rampHoldSeconds` is comfortably longer than the measurement window's own
settling behaviour, and check publish delay in the run that follows.

## Finding 6: achieved-rate seeding (D5) is worth ~12%, not the ~50% I claimed

Working note. `rampSeedFromAchievedRate` uses a failed hold's achieved throughput as the next
candidate instead of bisecting the bracket. I justified it in the review as the strongest available
lever on the ~48-minute integration test, guessing it "would roughly halve" discovery. That guess was
wrong. Measured against `FakeThroughputSystem` at 3-second polls and 45-second holds — the integration
test's own geometry — discovery cost, in wall-clock seconds:

| Capacity | Start rate | Bisecting | Seeded |  Change  |
|----------|------------|-----------|--------|----------|
| 841,000  | 5,000      | 768 s     | 678 s  | **−12%** |
| 4,500    | 5,000      | 408 s     | 408 s  | 0%       |

**Why it is only 12%.** The seed places `lo` accurately in one step but leaves `hi` where bracket's
last doubling put it — roughly 2x the true ceiling. Chop then has to bisect that whole span anyway, so
the seed saves the steps *below* the knee and none of the steps above it. Both trajectories still land
on the ceiling, so the change is sound; it is just not the lever I said it was.

**Why the second row is unchanged.** With the start rate already above capacity the bracket phase
halves *downward*, and that path is deliberately not seeded: halving is already geometric, and one
spurious low reading would drop the search orders of magnitude in a single step with nothing to
reopen `hi`. Anyone reading D5 as a fix for the near-zero-rate collapse should not.

**Measured but not landed.** The bracket's stale `hi` is the actual bottleneck, and the same
measurement bounds it. Under `THROUGHPUT` the gate passes only while `published >= ratio x expected`,
so a hold that achieved `a` implies no rate above `a / ratio` can pass — a *derived* upper bound, not
a guess. Spiking `hi = min(hi, achieved / ratio)` on top of the seed:

| Capacity | Bisecting | Seeded | Seeded + derived `hi` |
|----------|-----------|--------|-----------------------|
| 841,000  | 768 s     | 678 s  | **543 s (−29%)**      |

The cost is accuracy: the reported rate moves from 840,750 (0.03% under the true 841,000) to 819,975
(2.5% under), because a tighter `hi` converges on a lower point inside the band the 0.95 ratio
tolerates. It also leans harder on the assumption that capacity does not vary with the requested rate
— exactly the assumption run 3 above violated when the broker absorbed the overshoot into its buffers.
When that happens `achieved ~= requested`, so `min` makes the derivation a no-op rather than a
hazard, which is the reason it is safe to consider at all. Left as a decision, not landed: it trades
2.5% of reported accuracy for 17% of discovery time and belongs with the band-vs-point question rather
than inside a commit labelled D5.

## Finding 7: the trend gate works, is worth 2–5%, and corrects finding 5's conclusion

Working note. `THROUGHPUT` now also requires that a hold's second half published as fast as its
first (`secondHalfRate >= ratio x firstHalfRate`). Checks 1 and 2 average over the whole hold, which
dilutes a decline confined to the tail; comparing halves leaves it undiluted. Always on under
`THROUGHPUT` rather than behind a flag: `THROUGHPUT` is already opt-in and already documented as having
exactly this defect, so this is a fix to it, not a variant of it. No existing test changed behaviour.

**In isolation it is decisive.** A 25/45/90-second hold at 2000 msg/s against a 1000 msg/s ceiling,
with the absorption buffer sized to saturate 92% of the way through: aggregate ratio 0.96 (accepted),
trend ratio 0.92 (rejected). Parameterised over all three hold lengths to show the verdict no longer
depends on hold length *for that shape* of failure.

**Across a whole search it is worth much less.** Discovery driven to completion against a
20,000-message absorption buffer over a true 1,000 msg/s ceiling, each candidate given a clean buffer
(i.e. `rampDrainSeconds` working):

| Hold | Without trend gate | With trend gate | Error with gate |
|------|--------------------|-----------------|-----------------|
| 15 s | 2,316              | 2,264           | +126%           |
| 45 s | 1,440              | 1,366           | +37%            |
| 90 s | 1,217              | 1,189           | +19%            |

The gate is worth 2–5%. Hold length is worth 6x. The reason: accepted candidates are the ones that
saturate *late* in their hold or not at all, and for those the decline is small in the second half
too. Both gates are 5% tests; concentrating one into half the window roughly doubles its sensitivity
to a decline, which shifts the accepted rate only slightly because rate and saturation-fraction are
steeply related. Keep it — it is free, always conservative, and `trendRatio` in `FINDER-HOLD` is
diagnostic — but do not budget it as the fix.

**This corrects finding 5.** That entry concluded "longer holds do not fix it", from the integration
test's measurement-window publish delay barely moving (3,037 ms at a 15s hold to 2,612 ms at 45s).
That inference was wrong, because publish delay in a 120-second measurement window is mostly a
property of that window, not of the hold that chose the rate. Measured against a known ceiling
instead, hold length moves the answer a great deal: +126% to +19% for a 6x hold. It converges slowly —
roughly, the error halves per doubling of the hold, and even a 90-second hold is still 19% over — so
"longer holds are not sufficient" stands. "Longer holds do not help" does not.

**Still open.** Neither gate can see a hold that absorbs for its entire length, and no statistic
computed inside that hold can. The remaining candidates are all outside the hold: scale the hold with
the candidate rate (absorption time is roughly buffer-depth / overshoot, so higher rates need longer
holds, not equal ones), or give up on a point value and report the band the sweep actually supports.

## Finding 8: a full AKS run across all three transport arms, with the 180s-hold defaults

Found running the consuming harness's (`conduktor/benchmarks`) AKS integration test, 2026-07-27,
`representative` infra preset, `BACKLOG` verdict (still the default). First run of this matrix
across all three transport arms in one dispatch — `direct` (no gateway), `transparent` (gateway,
no interceptors), `encrypt` (gateway + real Vault transit KMS) — rather than `direct` alone as in
every entry above. One "chop" load shared across arms (`rampStartRate` is a load-level field, not
per-arm, so one value has to serve all three):

```yaml
rampStartRate: 5000
rampMaxBacklogSeconds: 0.5
rampBracketHoldSeconds: 90
rampHoldSeconds: 180
rampConvergenceTolerance: 0.05
rampSeedFromAchievedRate: true
rampMaxDiscoveryMinutes: 35
testDurationMinutes: 40
```

**Result**: `direct` confirmed **588,930 msg/s** (`nonMonotonic: false`, discovery ~27min),
`encrypt` confirmed **76,425 msg/s** (`nonMonotonic: false`, discovery ~15min — fewer bracket
doublings needed for its lower ceiling). Both then held flat through their full measurement
windows (`direct`: 588k-591k across 239 samples; `encrypt`: 76.2k-76.7k across 239 samples) — no
drift, no decline. `transparent`'s result never made it off the cluster: a 0-byte `result.json`,
no coordinator log, no metrics — the harness's `kubectl cp` step failed to copy the file even
though `wait_job` had already confirmed the underlying Job completed (`kubectl cp`'s exit code is
not trustworthy for this — kubernetes/kubectl#199). Harness-side, not a CHOP defect; the harness
now retries that copy step up to 3 times before giving up, prompted directly by hitting this twice
in one day (this run's `transparent`, and an earlier run's `aimd`).

**These numbers matter beyond "it confirmed cleanly".** Every prior AKS trial in this log used
`AIMD` as the comparison point on the same or similar hardware, and AIMD never got past the
28,000-90,000 msg/s range, oscillating rather than settling. `direct`'s 588,930 here is not a
noisier version of that number — it is roughly **6.5-20x** higher, on the *same class of
hardware*, because AIMD's continuous reactive control never had a chance to explore that high
before its own backlog limits (environment-variable-only, not rate-relative) forced it back down.
This is the clearest evidence in this log that CHOP is not just "AIMD but reproducible" — on a
sufficiently capable cluster it finds a materially different, and materially more correct, answer.

**A visual record exists for `direct` and `encrypt`** (not `transparent`, given the collection
failure above): `FINDER-POLL` lines (one per poll, `t`/`rate`/`achieved`/`backlog`/`delayP99Ms`/
`latencyP99Ms`) plotted alongside the same window's Prometheus broker/gateway CPU, memory and GC
pause, six stacked panels sharing one time axis. Both plots make the "Known limitation" section's
overshoot claim directly visible rather than inferred: `encrypt`'s backlog spikes to ~150,000
messages and e2e p99 latency to ~3 seconds at the exact moment bracket's doubling overshoots past
the true knee, before chop backs off and finds it; `direct`'s broker memory climbs to a hard
~8 GiB ceiling and pins there for the rest of discovery — page-cache saturation, visible as a flat
line, not a number in a log. Script + PNGs live outside this repo (harness-side artifact, not
committed here): `conduktor/benchmarks`'s workspace, `.context/ramp-reports/`.

## Finding 9: where discovery's time actually goes, and how much of it is unearned rigor

Working note, drawn from the same `finder-chop-3arms` AKS run as Finding 8 — reconstructed by
grouping consecutive `FINDER-POLL` lines by their `rate=` value to get the exact wall-clock cost
of every bracket step, chop step, and failure, rather than reasoning from the plots alone.

**How fast backlog actually arises once a candidate exceeds capacity.** `encrypt`'s only real
overload (80,000 -> 160,000, the step that found its knee) went from a healthy baseline straight
to failure in a single poll:

| t (s) | target  | achieved |      backlog       |
|-------|---------|----------|--------------------|
| 491   | 80,000  | 80,102   | 1,299 (healthy)    |
| 494   | 160,000 | 131,332  | **74,835**         |
| 510   | 87,151  | 97,993   | **232,102 (peak)** |
| 522   | 80,447  | 97,197   | 1,598 (recovered)  |

One 3-second poll is enough to go from a stable ~1,300 messages to ~74,800 (~57x) the moment a
candidate genuinely exceeds capacity — not a gradual creep, a step function. It keeps growing for
another ~12-15s (the finder is still reacting) before recovering to baseline by ~28s. Every
*healthy* doubling in both arms, by contrast, never exceeded a few thousand messages and showed no
trend across its full hold. This is a clean, fast, unambiguous signal — which matters for the
recommendations below, because it means a real failure does not need a long hold to be seen; it
needs a long hold only to make sure a *pass* is not a slow-building one (see the bracket-hold
finding below, and "Known limitation" in `RATE_FINDING.md`).

**Where the time went, `direct` (1,606s / 26.8min total discovery):**

|                       Phase                        | Candidates |                 Cost                 |
|----------------------------------------------------|------------|--------------------------------------|
| Settle + 1st bracket hold (5,000)                  | 1          | 120s                                 |
| Bracket doublings (10k -> 320k, all clean)         | 6          | 6 x 89s = 534s                       |
| Bracket failure (640,000)                          | 1          | 22s (fail-fast, no full hold needed) |
| Chop bisection (480k, 560k, 600k, 620k, all clean) | 4          | 4 x 178s = 712s                      |
| Confirm (589,000 = 620,000 x 0.95)                 | 1          | 178s                                 |

Every bracket/chop hold that passed ran for its *entire* configured length (89s / 178s) — clean
holds are not shortened early. The 640,000 failure is the only step that returned in less than a
full hold, because a breach is fail-fast by design.

**Where the time went, `encrypt` (882s / 14.7min total discovery):** identical shape through the
4 clean doublings (10k/20k/40k/80k, 4 x 89s = 356s) and the settle+first-candidate 120s, then
diverges sharply at its knee. The 160,000 candidate fails instantly (0s), and rather than a
bisection, `rampSeedFromAchievedRate` re-seeds from the achieved throughput of the failed
candidate — but the *next eight* candidates (131332, 119245, 108605, 94302, 87151, 83576, 81788,
80894) each fail in 0-3s too, before 80,447 finally holds clean for a full 178s. That cascade cost
only ~27s total to cross from 160,000 down to a stable ~80,000 — far cheaper than a bisection would
have been. **But it is not for the reason it looks like.** Backlog at the moment 80,894 was tried
was still >180,000 (see the table above) — that candidate did not fail because 80,894 msg/s is
unsustainable, it failed because the *previous* candidate's overshoot had not drained yet. The
cascade landed close to the right answer, but on this evidence it is not clear it always would:
`rampDrainSeconds` is 0 (off) in this matrix, so nothing separates one candidate's fallout from the
next one's verdict. This is exactly the cross-candidate contamination the "Known limitation"
section already names for the bracket phase specifically; this run shows it happening across
*rejected* candidates during a seeded recovery too.

**Recommendations, ranked by evidence strength (harness-side matrix tuning, not algorithm changes):**

1. **Cut the fixed measurement window — the single biggest lever.** Both arms ran the full
   `testDurationMinutes: 40` after discovery (239 x 10s samples each) and showed *zero* drift the
   entire time (see Finding 8's plots). Total cell time was discovery + measurement:
   `direct` 26.3min + 40min, `encrypt` 14.7min + 40min — measurement dominates both, and nothing in
   this data suggests 40 minutes shows anything a much shorter window would not. This is the
   highest-confidence cut available: it is supported by watching the *entire* window stay flat,
   not by extrapolating from a partial one.
2. **Start closer to the known range, per arm.** `rampStartRate` is one value shared across all
   three arms (a load-level field, not per-arm), so it was set once (5,000) as a compromise for
   very different ceilings. Every doubling below ~1/8th of the eventual answer held cleanly with
   flat, unremarkable backlog — zero information gained beyond "still fine." Splitting into
   per-arm loads and starting direct/transparent near 300-400k, encrypt near 40-50k (informed by
   *this run's own* confirmed rates) would skip 4-5 of those doublings per arm for free.
3. **Turn `rampDrainSeconds` on.** Per the finding above, the fast reseed cascade currently trades
   confidence for speed without saying so. Draining is cheap when unneeded (one poll) and directly
   closes the cross-candidate contamination this run's own data shows happening.
4. **`rampBracketHoldSeconds` (90s) likely exceeds what the non-final bracket steps need.** Every
   clean bracket hold's backlog was already representative within the first 1-2 polls (3-6s); none
   showed a trend building across the remaining ~85s. The 90s of scrutiny plausibly only matters
   for the step nearest the knee, where page-cache absorption can mask trouble longer. A shorter
   value trades some of that margin for time, backstopped by chop's full-rigor re-examination of
   the same territory and the reopen-on-failed-confirm path.
5. **`rampConvergenceTolerance` is a smaller, real trade.** `direct` needed exactly 4 bisections to
   narrow a 2x-wide bracket to 5%; loosening to ~10% would likely drop one (~178s) at the cost of a
   wider confirmed-rate band.

Items 1 and 2 together are argued directly from data that stayed flat for its full duration or
carried zero new information — no confidence given up to get them. Items 3-5 trade a specific,
named risk for time; 3 is closing a gap this run's data exposes rather than opening one.

## Finding 10: both AKS arms under-report, for two different reasons

Working note, from the same `finder-chop-3arms` run as Findings 8 and 9, re-examined poll-by-poll
rather than from the confirmed rates. Both arms confirmed with `nonMonotonic: false` and both held
flat for their full 40-minute windows, so both are *safe*. Neither is the ceiling.

**`direct`: a single poll capped it.** `hi` was set at 640,000 twenty-four seconds into a 90-second
bracket hold:

| t (s) | achieved | receive backlog |  publish shortfall / poll   |
|-------|----------|-----------------|-----------------------------|
| 678   | 632,703  | 5,113           | 21,891                      |
| 681   | 640,034  | 3,430           | -102                        |
| 685   | 639,586  | 6,049           | 1,242                       |
| 688   | 629,694  | 5,123           | 30,918                      |
| 691   | 650,592  | 6,249           | -31,776                     |
| 694   | 637,371  | 5,100           | 7,887                       |
| 697   | 642,043  | 6,573           | -6,129                      |
| 700   | 603,315  | 0               | **110,055** (limit 100,000) |

Seven healthy polls, then one 3-second sample dipped 5.7% and exceeded the limit by 10%. The hold's
own aggregate was `published/expected = 15,618,179 / 15,755,821 = 99.13%`. Nothing ever reopens `hi`,
so **588,930 is a floor, not the answer** — the arm demonstrably sustained ~634,000 for 24 seconds.
Broker CPU never passed 0.45 of 8 cores, which corroborates it.

**`encrypt`: eight candidates were rejected without being measured.** Finding 9 records the cascade
and correctly diagnoses it as the previous candidate's undrained overshoot. Adding the magnitude: at
160,000 the gateway *achieved* 131,332 msg/s, and gateway CPU peaked at 1.8 of its 2 allocated cores.
That looks like the 2-core encryption ceiling, which puts real capacity near 110,000-130,000 against a
confirmed 76,425 — the confirmed figure uses only 1.2 of 2 cores. **So the direct:encrypt penalty
reported as 7.7x is probably nearer 5x**, and that ratio is the number to be most careful quoting.

**A configuration trap sat underneath both.** The matrix set `rampMaxBacklogSeconds: 0.5` and, on the
`direct` arm, never got it:

```
direct  @589k:  0.5s = 294,465 msgs  ->  clamped to ceiling 100,000  (0.17s effective)
encrypt @76k:   0.5s =  38,212 msgs  ->  rate-scaled applies
```

Because the limit is `max(floor, min(rate x seconds, ceiling))`, the old 100,000 ceiling default won
above roughly 200,000 msg/s and the rate-scaled limit silently became a fixed count again — the exact
thing rate-scaling replaces. It is also the direct cause of the false failure above: 110,055 tripped a
limit that should have been 320,000. Neither this run's own findings nor the first pass of this review
spotted it.

**Fixed since:** `rampBreachPolls` (default 2) so one sample cannot condemn a candidate;
`rampMaxBacklogCeiling` default raised 100,000 -> 500,000 so 0.5s means 0.5s up to 1,000,000 msg/s.
Still open: `rampDrainSeconds` remains off in every matrix run so far, which is what the `encrypt`
cascade needs.

## Finding 11: making discovery quicker without giving up rigor

Finding 9's recommendation 4 was to shorten `rampBracketHoldSeconds`, on the evidence that clean
bracket holds were representative within 1-2 polls and showed no trend over the remaining ~85s. That
reasoning is sound about the *data* and unsafe as stated: a short hold is exactly what cannot see
slow-building absorption, `lo` only ever moves upward, and the same run's `direct` plot shows broker
memory climbing to a hard 8 GiB and pinning there — the absorption reservoir filling, visible as a
flat line. Hold length is also the dominant error term measured anywhere in this log (Finding 7:
+126% at 15s, +37% at 45s, +19% at 90s against a known ceiling).

**What makes it safe: re-verify `lo` at full length before chopping.** The probe locates the bracket;
the full hold certifies it. One extra hold, and the objection goes away — an over-confirmed probe now
fails its re-verification, `hi` becomes that `lo`, and the search drops back to the highest recorded
pass below it. Measured on this run's geometry against a hard 589,000 msg/s ceiling:

| `rampBracketHoldSeconds` | discovery | vs 90s | confirmed rate |
|--------------------------|-----------|--------|----------------|
| 90 (this run's value)    | 1,494s    | —      | identical      |
| 45                       | 1,134s    | −24%   | identical      |
| 20                       | 942s      | −37%   | identical      |
| 9                        | 846s      | −43%   | identical      |

All four converge on the same rate, so the saving comes out of probing rather than out of rigor.

**Ranked for a quicker next run**, with what each costs:

1. **`rampBracketHoldSeconds: 20`** — −37% discovery, no measured accuracy cost now that `lo` is
   re-verified. The safest large cut available.
2. **Per-arm `rampStartRate`** (Finding 9's recommendation 2) — free. Every doubling below ~1/8 of the
   answer carried no information. Needs per-arm loads, which is harness-side.
3. **Cut `testDurationMinutes` from 40** — the biggest single block of wall-clock, but keep it at
   *several times* `rampHoldSeconds` rather than at minimum. That window is the independent check that
   a confirmed rate is sustainable; it is what catches "passed a hold, then 2.6s of publish delay"
   (Finding 5). Its flatness is the result being relied on, not a reason to stop measuring. ~10 minutes
   against a 180s hold keeps the check meaningful.
4. **`rampConvergenceTolerance: 0.10`** — drops roughly one chop hold, widens the reported band. A real
   trade, not a free one.

Not recommended: shortening `rampHoldSeconds` itself. That is the one lever measured to move the
answer rather than the clock.

## Finding 12: `rampMaxBacklogSeconds` means two different things, and one of them is loose

Working note, surfaced while measuring Finding 11 rather than from a trial. The same limit is compared
against two quantities of different dimension:

- `receiveBacklog` is **cumulative** — `subscriptions x totalPublished - totalReceived`. Against a
  limit of `rate x seconds`, that reads as "consumers are at most this many seconds behind". Tight and
  meaningful.
- `publishBacklog` is **per-period** — `expected - published` for this poll alone. Against the *same*
  limit, at a 3-second poll period, `rate x 0.5` tolerates a 17% publish shortfall **every period,
  indefinitely**, because nothing accumulates across polls.

So `rampMaxBacklogSeconds: 0.5` asks for half a second of consumer lag and simultaneously permits a
permanent one-sixth producer shortfall. Raising the ceiling default (Finding 10) makes the publish side
looser still, and the old 100,000 clamp had been accidentally masking it at high rates.

Not fixed, and worth a decision rather than a quick patch. The obvious repair is to accumulate the
publish shortfall across the hold and compare that against the limit — at which point it becomes
`holdExpected - holdPublished`, i.e. the `THROUGHPUT` verdict's producer gate expressed as a count. So
the two verdicts are closer than they look, and the honest version of `BACKLOG` may be "cumulative
receive lag, plus THROUGHPUT's producer ratio".

## Finding 13: `BACKLOG` verdict trapped the transparent arm at 9,203 msg/s — 125x below its real ceiling

AKS, representative infra-preset, harness commit `aceb354` (`omb/matrices/finder-chop-verdict-3arms.yaml`),
run 30343759563, same PR-19 HEAD as Finding 11/12 (`26653c3`: `rampBreachPolls` consecutive-breach
requirement, `rampMaxBacklogCeiling` default 500,000, drain-by-default, re-verify-lo). 3 arms x 2 loads,
both loads on the same topology (`rampStartRate: 5000`, `rampBracketHoldSeconds: 20`,
`rampMaxDiscoveryMinutes: 50`, `testDurationMinutes: 10`): `chop-backlog` uses the default `BACKLOG`
verdict (`rampMaxBacklogSeconds: 0.5`), `chop-throughput` uses `rampVerdict: THROUGHPUT` with no
backlog fields set.

Confirmed rates (`rampVerification.rate`), same job, same cluster:

|     arm     |  BACKLOG  |  THROUGHPUT   |  ratio   |
|-------------|-----------|---------------|----------|
| direct      | 1,112,274 | 1,164,974     | 1.05x    |
| encrypt     | 128,258   | 148,432       | 1.16x    |
| transparent | **9,203** | **1,151,838** | **125x** |

direct and encrypt are consistent within the kind of spread a different bracket/chop path explains.
transparent is not. THROUGHPUT's transparent number sits within 1% of direct's own confirmed rate, and
its chart (`ramp-report-chop-throughput-transparent.png`) shows the saturation signature you'd expect
at a real ceiling: broker CPU climbing to 0.6–0.8 cores, publish-delay p99 spiking into the thousands of
ms during backlog bursts. BACKLOG's transparent chart (`ramp-report-chop-backlog-transparent.png`)
shows the opposite: broker CPU pinned at ~0.2–0.3 cores and gateway CPU ~0.1 cores for the entire
15-minute remainder of discovery after the first bracket doubling past 10,000 msg/s — nowhere near a
resource ceiling.

Root cause, from the raw polls (`chop-backlog/transparent.coordinator.log`):

```
09:36:36.053 FINDER-POLL t=92  rate=20000 achieved=20001 backlog=2183
09:36:39.138 FINDER-HOLD phase=BRACKET rate=20000.0 verdict=exceeded achievedRatio=0.957 bracket=[10000.0, null]
09:36:39.139 FINDER-POLL t=95  rate=20000 achieved=14179 backlog=1464
09:36:42.242 FINDER-POLL t=98  rate=10000 achieved=0    backlog=1464 delayMaxMs=0.0 latencyP99Ms=0.0
09:36:45.345 FINDER-POLL t=101 rate=10000 achieved=0    backlog=1464 delayMaxMs=0.0 latencyP99Ms=0.0
09:36:48.461 FINDER-HOLD phase=CHOP rate=10000.0 verdict=exceeded expected=62268 published=0 received=0 achievedRatio=0.000 bracket=[10000.0, 20000.0]
```

20,000 exceeds on a normal near-miss (`achievedRatio=0.957`), setting `hi=20000`. Per the re-verify-lo
mechanism, the next hold re-tests `lo=10000` before chopping — and that hold reads `achieved=0` on
every one of its 3 polls (9+ seconds; it aborted well short of a full `rampBracketHoldSeconds` window),
with `delayMaxMs`/`latencyP99Ms` both exactly `0.0`. That's not "fell short of the target," it's "the
stats recorder saw nothing at all." The immediately preceding poll (still labelled `rate=20000`) had
already dropped `20001 -> 14179` — the tail of the old rate draining as the transition happened. Nothing
else in the run — CPU, GC, broker or gateway saturation — supports a real ceiling anywhere near
10,000–20,000 msg/s on this arm; this reads as a producer-side stall or stats-recorder gap tied to the
downward rate transition itself, not a capacity limit.

`rampBreachPolls=2` only requires *consecutive* breaching polls within a hold — trivially satisfied when
every poll in a short hold reads zero. The bracket never recovered: chop proceeded to bisect entirely
within `[5000, 10000]`, converging on 9,203 — about 125x below the arm's real ceiling, independently
confirmed by the same job's `THROUGHPUT` cell for the same arm.

Notably, `chop-throughput/transparent` made the identical class of downward transition later in its own
discovery (1,280,000 → 640,000, also a bracket-exceeded → re-verify-lo step) with no stall: achieved
dropped smoothly `1,255,211 -> 1,110,551 -> 867,044` across the transition
(`chop-throughput/transparent.coordinator.log`, 11:11:30–11:11:37). So this isn't an inherent property
of downward rate transitions — it looks like a rare, arm/timing-specific hiccup, made catastrophic only
because BACKLOG's fast-fail treats "zero for a whole short hold" as a confirmed breach with no check for
"was anything published at all this hold."

Not fixed. Two independent angles worth considering upstream: (a) treat `achievedRatio == 0.000` as a
suspect measurement — distinguish "shortfall" from "recorded nothing" — rather than a confirmed breach,
or (b) require a stall to persist across a hold *at the new rate* rather than failing on a re-verify
hold that reads zero for its entire (short) duration. This is the same failure class as the pre-`ce3d4ee`
transparent-arm false-fail (2,152 msg/s, see above): those fixes closed the false-confirm shapes tested
at the time, but "every poll in the hold reads zero" still slips through a consecutive-breach check,
since consecutive-ness is trivially true when there's nothing but zero-polls in the hold.

Charts: `ramp-report-chop-backlog-transparent.png` / `ramp-report-chop-throughput-transparent.png`
(also `ramp-report-chop-{backlog,throughput}-{direct,encrypt}.png` for the two healthy arms, both
consistent within 5–16% across verdicts).

### Correction and root cause for Finding 13

Reviewed against `aks-omb-report8`'s raw artifacts. The headline holds — 9,203 msg/s is wrong by
roughly two orders of magnitude — but three things need correcting, one of them mine.

**It is an acknowledgement stall, not a recorder gap.** The excerpt above stops one poll short of the
evidence:

```
t=104  rate=10000  achieved=0       delayMaxMs=0.0   latencyP99Ms=0.0
t=108  rate=5000   achieved=41278   delayMaxMs=28.3  latencyP99Ms=10681.9
```

41,278 msg/s against a 5,000 target, p99 publish latency 10.7 seconds. Nothing was lost; about nine
seconds of acknowledgements were *deferred* and then arrived at once. That also explains why all three
signals read exactly `0.0` together rather than one of them looking odd:
`WorkerStats.recordProducerSuccess` increments `messagesSent` **and** records both latency histograms at
the same moment, on ack. An ack stall therefore zeroes every counter the finder can see, while the
messages are still in flight.

This is the same blind spot as the drain's, from the other side: in-flight-but-unacked work is invisible
to every counter `poll()` receives. There it causes a false "recovered"; here a false "breach".

**The re-verify-lo mechanism turned a bounded error into an octave.** Without it, 20,000 failing would
have left chop bisecting `[10000, 20000]`, the stall would have hit the 15,000 hold, and `lo` would have
stayed at 10,000 — the search continues in a bracket that still contains the truth. With it, the stall
lands on the re-verification of `lo` itself, so `hi` becomes 10,000, `lo >= hi` triggers the fallback to
`bestKnownPassBelow` = 5,000, and the bracket drops a whole octave to `[5000, 10000]`. The log confirms
the chain: `t=108 rate=5000` is the drain at the demoted `lo`, `t=111 rate=7500` the resumed midpoint.
Failing a re-verification destroys the only known-good footing, which makes a transient there maximally
expensive. That amplification is a property of the re-verification, not of the stall.

**`THROUGHPUT` is not a valid yardstick for the comparison.** Its own measurement windows disagree with
it:

|           cell           | confirmed | window median | avg publish delay | max backlog |
|--------------------------|-----------|---------------|-------------------|-------------|
| `BACKLOG` direct         | 1,112,274 | 1,110,419     | **6.5 ms**        | 24,144      |
| `BACKLOG` transparent    | 9,203     | 9,203         | 0.1 ms            | 1,955       |
| `BACKLOG` encrypt        | 128,258   | 128,261       | 0.0 ms            | 36,263      |
| `THROUGHPUT` direct      | 1,164,974 | 1,182,433     | **790.8 ms**      | 104,967     |
| `THROUGHPUT` transparent | 1,151,838 | 1,158,995     | **805.5 ms**      | 167,931     |
| `THROUGHPUT` encrypt     | 148,432   | 145,423       | 0.0 ms            | 59,219      |

~800 ms of average publish delay is unsustainable by the same threshold used everywhere else in this
log (the integration test's bar is 500 ms), so citing 1,151,838 as transparent's "real ceiling" repeats
the over-report of Findings 5 and 7. Transparent's sustainable ceiling is probably ~1.05-1.10M, by
analogy with `BACKLOG` direct's 1,112,274 at 6.5 ms — so 9,203 is about **120x** low, and the reference
should be `BACKLOG` direct rather than `THROUGHPUT` transparent.

Note also *where* `THROUGHPUT` breaks: it is exact at encrypt's 148,432 (0.0 ms) and 800 ms out at
1.1M+. Absorption capacity is roughly fixed in messages, so it conceals proportionally more the faster
the candidate — which is what Finding 7 predicted and this run confirms at two scales in one job.

**Two estimates of mine in Finding 10 were wrong**, in opposite directions:

- I put direct's real capacity at "≥634k". It is **1,112,274**. Correct as a floor, badly wrong as an
  estimate: the false failure truncated the bracket *climb*, not merely the final answer.
- I said the direct:encrypt penalty was "nearer 5x" than the reported 7.7x. It is **8.7x**
  (1,112,274 / 128,258). The encrypt half of that estimate was good — I predicted 110,000-130,000 against
  an actual 128,258 — but direct rose far more than I allowed for, so the correction went the wrong way.

**What the run does validate:** `direct` went from 588,930 (Finding 8, with the 100,000 ceiling and
single-poll fast-fail) to 1,112,274 here, at 6.5 ms publish delay and a bounded 24,144 backlog. The
ceiling raise and the consecutive-breach requirement are worth 1.89x on that arm, and the result holds
for its full measurement window.

**Fixed since:** a poll that acknowledges nothing while expecting something is now treated as an absence
of data rather than a breach — not judged, and the hold restarts rather than resuming a window it did not
observe. A `FINDER-STALL` line records each one. Because `lo` is only demoted on a *measured* breach, the
octave-loss above cannot recur from this cause. Still open: the drain's version of the same blindness,
which needs publish delay passed into the finder.

## Finding 14: Finding 13 confirmed fixed — re-run against `e1481de`

Same matrix, same settings, same arms as Finding 13 (`finder-chop-verdict-3arms.yaml`, AKS
representative, run 30366818869), against PR-19 HEAD `e1481de` — i.e. `fde5838`/`e0b1d5f`/`965b3ca`
all in. transparent's `BACKLOG` result:

|                        | Finding 13 run |   this run    |
|------------------------|----------------|---------------|
| transparent BACKLOG    | 9,203          | **1,177,872** |
| transparent THROUGHPUT | 1,151,838      | 1,421,882     |
| direct BACKLOG         | 1,112,274      | 1,111,681     |
| direct THROUGHPUT      | 1,164,974      | 1,099,507     |
| encrypt BACKLOG        | 128,258        | 119,670       |
| encrypt THROUGHPUT     | 148,432        | 128,969       |

transparent BACKLOG went from 125x under the arm's real ceiling to matching it. Its chart
(`ramp-report-chop-backlog-transparent.png` from this run) now shows what direct's and encrypt's
always have: a full bracket doubling, real saturation at confirm (gateway CPU 0.8–0.95 cores — this
arm's actual bottleneck), no collapse. direct and encrypt hold within normal run-to-run spread
(1–13%), as they did across the two BACKLOG-verdict runs already logged.

One new thing surfaced by the comparison rather than by either run alone: this run's transparent
THROUGHPUT result (1,421,882) exceeds this run's own direct THROUGHPUT result (1,099,507) by ~29% —
the gateway passthrough out-throughput-ing bypassing it. Checked the raw hold trace before writing
this down: it's not a repeat of Finding 13's shape. Bracket climbed to 2,560,000 before exceeding
(higher than direct's own bracket ceiling of 1,280,000 in the same run), 8 holds of chopping,
`nonMonotonic=false`, and the chart shows real saturation (broker CPU 0.6–0.8 cores, same shape as
direct's). Legitimate result, not a glitch — just unexplained. Not investigated further; flagging in
case it recurs or someone has a mechanism (connection multiplexing through the gateway vs. many
direct client connections is the only candidate that's occurred to me, unconfirmed).

## Finding 15: encrypt's ceiling is the gateway's CPU limit, not the broker — and broker MEM is a dead signal

Resource attribution from the Finding 14 re-run (30366818869), peak CPU/MEM per pod over each cell's
discovery window:

|  verdict   |     arm     | confirmed | broker CPU (max) | broker MEM (max) | gateway CPU (max) | gateway MEM (max) |
|------------|-------------|----------:|-----------------:|-----------------:|------------------:|------------------:|
| BACKLOG    | direct      | 1,111,681 |             0.71 |         7.91 GiB |                 — |                 — |
| BACKLOG    | transparent | 1,177,872 |             0.75 |         7.91 GiB |              0.95 |          0.55 GiB |
| BACKLOG    | encrypt     |   119,670 |             0.43 |         7.91 GiB |          **2.00** |          1.01 GiB |
| THROUGHPUT | direct      | 1,099,507 |             0.76 |         7.91 GiB |                 — |                 — |
| THROUGHPUT | transparent | 1,421,882 |             0.83 |         7.91 GiB |              0.61 |          1.04 GiB |
| THROUGHPUT | encrypt     |   128,969 |             0.48 |         7.91 GiB |          **2.00** |          1.12 GiB |

Two things fall out of this that weren't visible from the confirmed rate alone:

- **Encrypt's ceiling is the gateway's CPU request/limit, not the broker.** Gateway CPU sits at
  exactly 2.00 in both verdicts — the harness's `gateway-cpu` default, unchanged in this dispatch —
  while broker CPU is 0.43–0.48, nowhere near saturated. Encrypt's throughput (~120–130k, an order of
  magnitude below direct/transparent) isn't a Kafka-side or gateway-architecture limit; it's the
  encrypt interceptor's per-message CPU cost hitting a container CPU cap that was never raised for
  this test. The number this run reports for encrypt is a statement about `gateway-cpu: 2`, not about
  the algorithm or the interceptor's ceiling in the abstract — raising the limit is very likely to
  raise the confirmed rate, untested here.
- **transparent's gateway CPU has real headroom** (0.61–0.95 of the same 2-core cap) — it isn't
  CPU-bound at the gateway in either verdict, which is at least consistent with (though doesn't fully
  explain) transparent tracking or exceeding direct's throughput, per Finding 14.

Separately, **broker MEM is the same 7.91 GiB in all six cells** — that's the JVM heap sitting at its
allocated ceiling, not a per-workload footprint measurement. It doesn't move with rate, arm, or
verdict, so it isn't a comparison signal worth reading into for these runs; noting it so it isn't
mistaken for one later.

## Finding 16: transparent's THROUGHPUT result is the largest over-confirm in this log

Same run as Findings 14 and 15 (30366818869), re-examined by measurement window rather than by
confirmed rate. Finding 14 records transparent `THROUGHPUT` at 1,421,882 — 29% above the same run's
direct `THROUGHPUT` — and calls it "legitimate result, not a glitch — just unexplained". It is not
legitimate. Its own measurement window disagrees with it by a wide margin:

|           cell           | confirmed | window median | avg publish delay |               delay shape across 59 intervals                |
|--------------------------|-----------|---------------|-------------------|--------------------------------------------------------------|
| `BACKLOG` direct         | 1,111,681 | 1,115,760     | 1,016 ms          | ~0, excursion peaking 2,978 ms at interval 29, ~0 by the end |
| `BACKLOG` transparent    | 1,177,872 | 1,178,258     | **4.4 ms**        | flat, 0.7 ms both ends                                       |
| `BACKLOG` encrypt        | 119,670   | 119,632       | **0.0 ms**        | flat                                                         |
| `THROUGHPUT` direct      | 1,099,507 | 1,103,938     | 16.6 ms           | flat                                                         |
| `THROUGHPUT` transparent | 1,421,882 | 1,278,132     | **22,930 ms**     | 13 ms → 435 → 4,125 → 40,421 → **55,585 ms**                 |
| `THROUGHPUT` encrypt     | 128,969   | 127,609       | 0.0 ms            | flat                                                         |

transparent `THROUGHPUT`'s delay climbs monotonically to **55.6 seconds** and its window delivers 10%
less than the rate it confirmed. That is unbounded queue growth, and it is a worse over-confirm than
the 1.8M incident that motivated this whole exercise. `nonMonotonic` is false and the chart looks like
saturation because every check `THROUGHPUT` applies is blind to in-flight, unacknowledged work.

**And this supplies the mechanism Finding 14 left open.** The question was why gateway passthrough
appears to out-throughput bypassing the gateway; the guess was connection multiplexing. The answer is
in Finding 15's own table: transparent `THROUGHPUT` gateway MEM is **1.04 GiB against 0.55 GiB** for
transparent `BACKLOG` — nearly double, for a nominally identical passthrough. transparent is not
faster. A proxy in the path adds a whole extra buffering layer, so `THROUGHPUT` can confirm a *higher*
rate precisely because there is more room to hide unacked work. direct has no gateway, no extra buffer,
and 16.6 ms.

So `THROUGHPUT`'s error is not simply "grows with rate" (Finding 7) — it grows with **how much
buffering sits between producer and broker**. That is why the over-confirm lands on transparent and not
on direct at almost the same rate, and it is now evidenced twice.

The `BACKLOG` direct excursion in that table is a separate, milder thing: near-zero at both ends with a
sustained mid-window degradation that recovered. Not inherited from discovery, not unbounded. Either
cluster variance or a bistable episode at a rate sitting on the knee; the confirmed rate itself
reproduced to within 0.05% of the previous run.

## Finding 17: recovery could not clear a queue, because it ran at lo

Found in the same run's `FINDER-DRAIN` lines, which only became readable once `e0b1d5f` made recovery
producer-aware. Two of four recoveries on the direct arm:

```
FINDER-DRAIN capped after 182s at 1280000 (backlog 29198, delayP99 24437ms,
             consumerCaughtUp=true producerCaughtUp=false)
FINDER-DRAIN capped after 180s at 1158209 (backlog 0,     delayP99 10448ms,
             consumerCaughtUp=true producerCaughtUp=false)
```

Both ran the full 180-second cap and gave up with the consumer caught up and the producer still 10 and
24 seconds behind its own schedule.

The cause is structural rather than a tuning problem. Recovery ran at `lo`, on the reasoning that `lo`
is a rate known to be sustainable — true, and insufficient. `lo` means "keeps up", not "has spare
capacity". Clearing a queue needs arrival below service, so at `lo` the queue shrinks at
`capacity - lo`, which by construction is nearly zero: draining at ~1.16M against a ~1.17M ceiling is
about 1% of headroom, and a 10-second queue then needs on the order of 1,000 seconds.

**Fixed:** recovery now runs at half of `lo`. That gives 2x headroom, clearing a queue in roughly its
own duration. Nothing is evaluated during recovery and it ends as soon as both sides are caught up, so
running slower costs nothing — while dropping to near-zero would leave the system cold, which is the
transient `rampSettleSeconds` exists to avoid.

Worth noting the sequence: `e0b1d5f` fixed recovery's blindness to the producer, and that fix
immediately exposed a flaw it had been masking. Before it, all four of these recoveries would have
reported "recovery complete" within a couple of polls while the producer was 24 seconds behind.

**A correction to my own diagnosis.** I first read the `BACKLOG` direct window as a queue inherited
from discovery and decaying away, and proposed a post-confirm drain to fix it. That was wrong, and
wrong for an avoidable reason: I judged the shape from a first-half/second-half median, which for a
mostly-zero series with a mid-window excursion is meaningless — sorting each half discards exactly the
ordering that distinguishes "inherited and decaying" from "grew and recovered". The interval series
shows near-zero at both ends, so the window never started behind and a post-confirm drain would have
fixed nothing. The drain-rate defect above is the real one, and it was in the same data.

## Decisions taken, and what is still open

Recorded here because several findings above end in "worth a decision" and a reader should not have to
infer which way they went.

**Verdict: `BACKLOG`.** `THROUGHPUT`'s diagnosis was right and `BACKLOG` has since absorbed the fix — its
limit scales with rate, and its producer side is now the same ratio `THROUGHPUT` uses. What separates
them is what each can see, and that is settled by Findings 13 and 16: `BACKLOG` watches a level, which
rises the moment arrival exceeds service; `THROUGHPUT` watches flows, which stay healthy for as long as
something downstream absorbs the overshoot. `THROUGHPUT` is retained for the one shape it handles better
(a large *stable* standing backlog, where a level has no correct threshold). See `RATE_FINDING.md`.

**Report a point value, not a band.** This reverses the earlier lean. The case for a band was that the
verdict looked history-dependent: three of six local runs withheld, and two identical local `BACKLOG`
runs landed 37% apart. Both of those turned out to be defects rather than properties — the withholding
was mostly the confirm hold running *at* the knee (fixed by confirming at `lo x (1 - tolerance)`), and
the spread was the count-based limit misfiring at high rates (fixed by the rate-scaled default). What is
left reproduces:

|     arm     |   run 9   |  run 10   |   spread   |
|-------------|-----------|-----------|------------|
| transparent | 1,177,872 | 1,177,897 | **0.002%** |
| direct      | 1,111,681 | 1,156,448 | 4.0%       |
| encrypt     | 119,670   | 129,326   | 8.1%       |

A point value is defensible at that reproducibility. Note the two things it does *not* claim: the rate is
deliberately conservative (`lo x 0.95`), and its *sustainability* has more run-to-run variance than the
rate itself — `direct` showed a mid-window publish-delay excursion peaking at 2,978 ms in run 9 and
nothing comparable in run 10, at essentially the same confirmed rate. So the number reproduces; whether
a given window is clean does not, which is why the measurement window remains the independent check
rather than a formality.

**`rampConvergenceTolerance` stays at 0.05 for now.** Loosening to 0.10 is the only untaken speed lever
(~1 fewer chop hold, ~18% of discovery), and it costs a wider converged band plus a reported rate 10%
below `lo` instead of 5%. Deferred pending more real runs rather than decided on a single job — the
tolerance is also the safety margin, so this trades measured accuracy for time and there is no reason to
spend that before the wall-clock actually hurts.

**Still genuinely open**, and inherent rather than unfixed:

- A hold whose buffers absorb for its *entire* length cannot be distinguished from a healthy one by any
  statistic computed inside it. Only a longer hold helps, and `rampHoldSeconds` is now calibratable from
  one run's `FINDER-POLL` series rather than guessed (see `RATE_FINDING.md`).
- The verdict is not a pure function of rate. `rampDrainSeconds` removes cross-candidate contamination,
  which was most of it, but the same rate can still pass and fail on the same broker minutes apart. The
  `isNonMonotonic()` latch exists to surface exactly that rather than to fix it.

