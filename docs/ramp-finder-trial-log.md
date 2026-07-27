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

### Trial finding: hold length, not the verdict predicate, is now the dominant error

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
