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
