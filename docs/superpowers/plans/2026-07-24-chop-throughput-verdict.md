# CHOP `THROUGHPUT` verdict — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in `rampVerdict: THROUGHPUT` mode to CHOP that decides a candidate rate's pass/fail from scale-free achieved-rate and consumer-drain ratios instead of an absolute backlog-message count.

**Architecture:** Introduce a `RampVerdict` enum and two `Workload` fields. Refactor `RampRateFinder` so the per-hold clean/exceeded decision goes through a small `holdClean()` seam — preserving today's `BACKLOG` behavior byte-for-byte — then implement the `THROUGHPUT` seam using per-hold accumulated `expected`/`published`/`received`. All bracket/chop/confirm/reopen/safety-cap machinery is reused unchanged.

**Tech Stack:** Java 17, JUnit 5, AssertJ, Maven (surefire unit + failsafe IT), Testcontainers (Kafka), Lombok.

## Global Constraints

- Java 17 (repo pins `17.0.14-tem` via `.sdkmanrc`); `mvn` must run under JDK 17.
- Default `rampVerdict: BACKLOG` ⇒ **zero behavior change**; all 17 existing `RampRateFinderTest` cases must stay green after every task.
- `rampMinThroughputRatio` default `0.95`.
- No new `Worker` API (finder already receives cumulative `totalPublished`/`totalReceived`).
- `spotless:check`, `checkstyle`, `spotbugs` must pass (`mvn -pl benchmark-framework verify`); run `mvn -pl benchmark-framework spotless:apply` before committing.
- The Testcontainers IT stays gated: skipped by default, runs under `-Pintegration-tests`.
- Reuse the existing state machine — do not alter bracket doubling/halving, chop bisection, confirmation holds, reopen-on-failed-confirm, `finishWithBestKnown`, or `recordVerdict`/`nonMonotonic`.

## File Structure

- **Create** `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampVerdict.java` — the mode enum (mirrors `RampAlgorithm`).
- **Modify** `benchmark-framework/src/main/java/io/openmessaging/benchmark/Workload.java` — add `rampVerdict`, `rampMinThroughputRatio`.
- **Modify** `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java` — verdict seam + throughput predicate.
- **Modify** `benchmark-framework/src/test/java/io/openmessaging/benchmark/RampRateFinderTest.java` — throughput unit tests + fakes.
- **Modify** `benchmark-framework/src/test/java/io/openmessaging/benchmark/ChopRateFinderKafkaIT.java` — `THROUGHPUT` IT variant.
- **Modify** `benchmark-framework/RATE_FINDING.md` — document both verdict modes + the AKS motivation.

---

### Task 1: `RampVerdict` enum + `Workload` fields

**Files:**
- Create: `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampVerdict.java`
- Modify: `benchmark-framework/src/main/java/io/openmessaging/benchmark/Workload.java`
- Test: `benchmark-framework/src/test/java/io/openmessaging/benchmark/WorkloadVerdictDefaultsTest.java` (create)

**Interfaces:**
- Produces: `enum RampVerdict { BACKLOG, THROUGHPUT }`; `Workload.rampVerdict` (default `BACKLOG`), `Workload.rampMinThroughputRatio` (`Double`, nullable).

- [ ] **Step 1: Write the failing test**

Create `WorkloadVerdictDefaultsTest.java`:

```java
/*
 * Licensed under the Apache License, Version 2.0 (the "License"); ... (copy the standard
 * license header used by the other files in this package)
 */
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WorkloadVerdictDefaultsTest {
    @Test
    void rampVerdictDefaultsToBacklogAndRatioIsUnsetByDefault() {
        Workload workload = new Workload();
        assertThat(workload.rampVerdict).isEqualTo(RampVerdict.BACKLOG);
        assertThat(workload.rampMinThroughputRatio).isNull();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl benchmark-framework test -Dtest=WorkloadVerdictDefaultsTest -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: FAIL to compile — `RampVerdict` / fields do not exist.

- [ ] **Step 3: Create the enum**

`RampVerdict.java` (mirror `RampAlgorithm.java`, same license header):

```java
package io.openmessaging.benchmark;

/** How CHOP decides whether a held candidate rate is sustainable. See RATE_FINDING.md. */
public enum RampVerdict {
    /** Original: a candidate fails if publish/receive backlog exceeds a message-count limit. */
    BACKLOG,
    /** Achieved-rate + consumer-drain ratios (scale-free). */
    THROUGHPUT
}
```

- [ ] **Step 4: Add the `Workload` fields**

In `Workload.java`, next to the other `ramp*` fields (after `rampAlgorithm`), add:

```java
    public RampVerdict rampVerdict = RampVerdict.BACKLOG;

    // Minimum fraction of target throughput (and of consumer drain) a candidate must achieve over a
    // hold to count as clean under rampVerdict: THROUGHPUT. Null -> default 0.95 in RampRateFinder.
    public Double rampMinThroughputRatio;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -pl benchmark-framework test -Dtest=WorkloadVerdictDefaultsTest -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
mvn -pl benchmark-framework spotless:apply
git add benchmark-framework/src/main/java/io/openmessaging/benchmark/RampVerdict.java \
        benchmark-framework/src/main/java/io/openmessaging/benchmark/Workload.java \
        benchmark-framework/src/test/java/io/openmessaging/benchmark/WorkloadVerdictDefaultsTest.java
git commit -m "feat(ramp): add RampVerdict enum and workload fields"
```

---

### Task 2: Refactor `RampRateFinder` to a `breachedNow` + `holdClean()` seam (BACKLOG-preserving)

Pure refactor: route the pass/fail decision through a `holdClean()` method that returns `true` for `BACKLOG` (a breach already fast-fails per-poll, so hold completion == clean today). No behavior change; the existing 17 tests are the regression gate.

**Files:**
- Modify: `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java`
- Test: `benchmark-framework/src/test/java/io/openmessaging/benchmark/RampRateFinderTest.java` (existing, unchanged — used as the gate)

**Interfaces:**
- Produces (private): `boolean holdClean()`; `pollBracket(boolean breachedNow, long periodNanos)`, `pollChop(boolean breachedNow, long periodNanos)`.

- [ ] **Step 1: Add the `holdClean()` seam and a verdict field**

Add field near the other `final` config fields (top of class):

```java
    private final RampVerdict verdict;
```

In the constructor (after `requiredConfirmationHolds` is set):

```java
        this.verdict = workload.rampVerdict != null ? workload.rampVerdict : RampVerdict.BACKLOG;
```

Add the seam (place it just above `recordVerdict`):

```java
    // Whether the just-completed hold counts as clean. For BACKLOG this is always true: a real
    // breach fast-fails per-poll before the hold ever completes, so reaching completion means clean.
    // THROUGHPUT overrides this in a later task.
    private boolean holdClean() {
        return true;
    }
```

- [ ] **Step 2: Rewrite `pollBracket` to branch on `holdClean()` at completion**

Replace the whole `pollBracket` method with (behavior-identical for BACKLOG — `failed` collapses to `breachedNow` since `holdClean()` is `true`):

```java
    private boolean pollBracket(boolean breachedNow, long periodNanos) {
        boolean failed;
        if (breachedNow) {
            failed = true;
        } else {
            elapsedHoldNanos += periodNanos;
            if (elapsedHoldNanos < bracketHoldNanos) {
                return false;
            }
            failed = !holdClean();
        }
        elapsedHoldNanos = 0;
        recordVerdict(currentRate, !failed);
        if (failed) {
            hi = currentRate;
        } else {
            lo = currentRate;
        }

        if (lo != null && hi != null) {
            phase = Phase.CHOP;
            currentRate = (lo + hi) / 2.0;
            return false;
        }

        bracketIterations++;
        if (bracketIterations >= MAX_BRACKET_ITERATIONS) {
            finishWithBestKnown();
            return true;
        }

        currentRate = failed ? currentRate / 2.0 : currentRate * 2.0;
        return false;
    }
```

- [ ] **Step 3: Rewrite `pollChop` to branch on `holdClean()` at completion**

Replace the whole `pollChop` method with (the failed path is the former `backlogExceeded` path verbatim; the clean path is the former completion path verbatim):

```java
    private boolean pollChop(boolean breachedNow, long periodNanos) {
        boolean failed;
        if (breachedNow) {
            failed = true;
        } else {
            elapsedHoldNanos += periodNanos;
            if (elapsedHoldNanos < holdNanos) {
                return false;
            }
            failed = !holdClean();
        }

        if (failed) {
            recordVerdict(currentRate, false);
            elapsedHoldNanos = 0;
            if (confirming) {
                hi = currentRate;
                lo = bestKnownPassBelow(hi);
                confirming = false;
                confirmationHoldsPassed = 0;
                currentRate = (lo + hi) / 2.0;
                return false;
            }
            hi = currentRate;
            currentRate = (lo + hi) / 2.0;
            return false;
        }

        recordVerdict(currentRate, true);
        elapsedHoldNanos = 0;
        if (!confirming) {
            lo = currentRate;
        }

        if ((hi - lo) / lo <= convergenceTolerance) {
            if (!confirming) {
                confirming = true;
                confirmationHoldsPassed = 0;
                currentRate = lo;
                return false;
            }
            confirmationHoldsPassed++;
            if (confirmationHoldsPassed >= requiredConfirmationHolds) {
                currentRate = lo;
                confirmed = true;
                phase = Phase.DONE;
                return true;
            }
            currentRate = lo;
            return false;
        }

        confirming = false;
        currentRate = (lo + hi) / 2.0;
        return false;
    }
```

- [ ] **Step 4: Rename the `poll()` dispatch variable for clarity**

In `poll()`, the local currently named `backlogExceeded` is the per-poll fast-fail signal. Rename it to `breachedNow` at its declaration and both `pollBracket`/`pollChop` call sites (lines ~165 and ~189). The computation (the `maxBacklogSeconds` / else block) is unchanged in this task.

```java
        boolean breachedNow;
        if (maxBacklogSeconds != null) {
            double limit =
                    Math.max(
                            maxBacklogFloor,
                            Math.min(currentRate * maxBacklogSeconds, (double) maxBacklogCeiling));
            breachedNow = receiveBacklog > limit || publishBacklog > limit;
        } else {
            breachedNow =
                    receiveBacklog > receiveBacklogLimit || publishBacklog > publishBacklogLimit;
        }

        return phase == Phase.BRACKET
                ? pollBracket(breachedNow, periodNanos)
                : pollChop(breachedNow, periodNanos);
```

- [ ] **Step 5: Run the full existing finder suite to verify no behavior change**

Run: `mvn -pl benchmark-framework test -Dtest=RampRateFinderTest -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: PASS — `Tests run: 17, Failures: 0, Errors: 0`.

- [ ] **Step 6: Commit**

```bash
mvn -pl benchmark-framework spotless:apply
git add benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java
git commit -m "refactor(ramp): route verdict through holdClean() seam (no behavior change)"
```

---

### Task 3: Implement the `THROUGHPUT` verdict + unit tests

Add per-hold accumulation and the ratio predicate. For `THROUGHPUT`, `breachedNow` is always `false` (verdict decided at hold completion) and `holdClean()` uses the accumulated ratios.

**Files:**
- Modify: `benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java`
- Test: `benchmark-framework/src/test/java/io/openmessaging/benchmark/RampRateFinderTest.java`

**Interfaces:**
- Consumes: `RampVerdict`, `verdict`, `holdClean()` seam (Task 2), `Workload.rampMinThroughputRatio` (Task 1).

- [ ] **Step 1: Write the failing throughput unit tests**

Append to `RampRateFinderTest.java` (before the trailing `}`), plus a shared helper `throughputWorkload()` and two fakes:

```java
    private static Workload throughputWorkload() {
        Workload workload = new Workload();
        workload.rampVerdict = RampVerdict.THROUGHPUT;
        workload.rampMinThroughputRatio = 0.95;
        workload.rampSettleSeconds = 0;
        workload.rampBracketHoldSeconds = 1; // one 1s poll completes a hold
        workload.rampHoldSeconds = 1;
        workload.rampConvergenceTolerance = 0.05;
        return workload;
    }

    @Test
    void throughputVerdictConvergesNearProducerCapacity() {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        // Producer caps at 4500 msg/s; consumer never the bottleneck.
        FakeThroughputSystem system = new FakeThroughputSystem(4500, 1_000_000_000L, 0);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(Math.abs(4500.0 - finder.getCurrentRate()) / 4500.0).isLessThan(0.1);
    }

    @Test
    void throughputVerdictRejectsRatesWhereTheConsumerCannotKeepUp() {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        // Producer could do 100k, but the consumer only drains 3000 msg/s -> consumer is the ceiling.
        FakeThroughputSystem system = new FakeThroughputSystem(100_000, 3000, 0);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        // Discovery is pinned by consumer drain (~3000), far below producer capacity.
        assertThat(finder.getCurrentRate()).isLessThan(6000.0);
        assertThat(finder.getCurrentRate()).isGreaterThan(1500.0);
    }

    @Test
    void throughputVerdictIgnoresLargeButStableInFlightBacklogThatACountLimitWouldReject() {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        // Producer caps at 50k; a stable 50,000-message in-flight backlog is always present, and the
        // consumer merely keeps pace with it. A 1000-count receiveBacklog check would reject every
        // rate; THROUGHPUT (drain ratio ~1.0) climbs to the real 50k ceiling.
        FakeThroughputSystem system = new FakeThroughputSystem(50_000, 1_000_000_000L, 50_000);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 400 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(Math.abs(50_000.0 - finder.getCurrentRate()) / 50_000.0).isLessThan(0.1);
    }

    /**
     * A producer/consumer pair with independent sustained capacities and an optional pre-established,
     * stable in-flight backlog (the consumer keeps pace with it but never closes it). Backlog is
     * therefore large-but-constant when seeded -- the case an absolute-count verdict mishandles.
     */
    private static final class FakeThroughputSystem {
        private final double producerCapacity;
        private final double consumerCapacity;
        long totalPublished;
        long totalReceived;

        FakeThroughputSystem(double producerCapacity, double consumerCapacity, long standingBacklog) {
            this.producerCapacity = producerCapacity;
            this.consumerCapacity = consumerCapacity;
            this.totalPublished = standingBacklog; // seed a stable in-flight backlog
            this.totalReceived = 0;
        }

        void advance(double rate, long periodNanos) {
            double periodSeconds = periodNanos / 1e9;
            totalPublished += (long) (Math.min(rate, producerCapacity) * periodSeconds);
            long lag = totalPublished - totalReceived;
            totalReceived += (long) Math.min(consumerCapacity * periodSeconds, lag);
        }
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -pl benchmark-framework test -Dtest=RampRateFinderTest -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: FAIL — the three new tests do not converge as asserted, because `holdClean()` still always returns `true` and `breachedNow` still uses the count check (so `THROUGHPUT` behaves like an unbounded-tolerance backlog run).

- [ ] **Step 3: Add per-hold accumulation to `poll()`**

Add fields (near `previousTotalPublished`):

```java
    private final double minThroughputRatio;
    private long previousTotalReceived = 0;
    private long holdExpected = 0;
    private long holdPublished = 0;
    private long holdReceived = 0;
```

In the constructor:

```java
        this.minThroughputRatio =
                workload.rampMinThroughputRatio != null
                        ? workload.rampMinThroughputRatio.doubleValue()
                        : 0.95;
```

In `poll()`, inside the settle block, keep the received baseline current too (next to `previousTotalPublished = totalPublished;`):

```java
            previousTotalReceived = totalReceived;
```

After the per-period `published`/`receiveBacklog`/`publishBacklog` are computed and `previousTotalPublished` is updated, add received-delta tracking and hold accumulation:

```java
        long received = totalReceived - previousTotalReceived;
        previousTotalReceived = totalReceived;

        // A fresh hold starts whenever elapsedHoldNanos was reset to 0 by the previous poll's
        // transition (or after settle). Reset the per-hold accumulators at that first poll.
        if (elapsedHoldNanos == 0) {
            holdExpected = 0;
            holdPublished = 0;
            holdReceived = 0;
        }
        holdExpected += expected;
        holdPublished += published;
        holdReceived += received;
```

- [ ] **Step 4: Make `breachedNow` mode-aware and implement `holdClean()`**

In `poll()`, guard the fast-fail computation so `THROUGHPUT` never fast-fails:

```java
        boolean breachedNow;
        if (verdict == RampVerdict.THROUGHPUT) {
            breachedNow = false; // throughput verdict is decided at hold completion (see holdClean)
        } else if (maxBacklogSeconds != null) {
            double limit =
                    Math.max(
                            maxBacklogFloor,
                            Math.min(currentRate * maxBacklogSeconds, (double) maxBacklogCeiling));
            breachedNow = receiveBacklog > limit || publishBacklog > limit;
        } else {
            breachedNow =
                    receiveBacklog > receiveBacklogLimit || publishBacklog > publishBacklogLimit;
        }
```

Replace the `holdClean()` body from Task 2 with:

```java
    private boolean holdClean() {
        if (verdict != RampVerdict.THROUGHPUT) {
            return true;
        }
        boolean producerKeepsUp = holdPublished >= minThroughputRatio * holdExpected;
        boolean consumerKeepsUp = holdReceived >= minThroughputRatio * holdPublished;
        return producerKeepsUp && consumerKeepsUp;
    }
```

- [ ] **Step 5: Run the full finder suite (new + existing)**

Run: `mvn -pl benchmark-framework test -Dtest=RampRateFinderTest -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: PASS — the original 17 plus the 3 new throughput tests (`Tests run: 20`).

- [ ] **Step 6: Commit**

```bash
mvn -pl benchmark-framework spotless:apply
git add benchmark-framework/src/main/java/io/openmessaging/benchmark/RampRateFinder.java \
        benchmark-framework/src/test/java/io/openmessaging/benchmark/RampRateFinderTest.java
git commit -m "feat(ramp): THROUGHPUT verdict (achieved-rate + consumer-drain ratios)"
```

---

### Task 4: Document both verdict modes in `RATE_FINDING.md`

**Files:**
- Modify: `benchmark-framework/RATE_FINDING.md`

- [ ] **Step 1: Add a "Verdict modes" subsection under CHOP**

After the CHOP "Algorithm" section, add a subsection documenting `rampVerdict`:

- `BACKLOG` (default): the existing backlog-count predicate (`rampMaxBacklogSeconds`/`Floor`/`Ceiling`, or the fixed `rampPublishBacklogLimit`/`rampReceiveBacklogLimit`). Note its structural weakness — a healthy pipeline's in-flight backlog scales with throughput, so any fixed-ish count is too strict at high rates and too loose at low ones.
- `THROUGHPUT` (opt-in): per hold, `clean ⇔ published ≥ ratio·expected AND received ≥ ratio·published`, with `ratio = rampMinThroughputRatio` (default 0.95). Scale-free (one threshold at every rate); no fast-fail (verdict at hold completion). Note it does not solve hold-length sensitivity (a hold must exceed the broker's burst-absorption time in either mode).

- [ ] **Step 2: Add the two new fields to the config table**

Add rows for `rampVerdict` (default `BACKLOG`) and `rampMinThroughputRatio` (default `0.95`, "CHOP only — THROUGHPUT verdict").

- [ ] **Step 3: Add a Trial-finding note recording the AKS motivation**

Add a short subsection recording the 2026-07-24 AKS re-validation (run 30103329337): on one cluster, BACKLOG-based methods under-reported the true sustainable rate (AIMD ~13.5k, `chop-floor` 2,734) while a sustained ~880k held cleanly for 15 minutes — the evidence that motivated the scale-free THROUGHPUT verdict.

- [ ] **Step 4: Verify formatting**

Run: `mvn -pl benchmark-framework spotless:apply` then `git diff --stat benchmark-framework/RATE_FINDING.md`
Expected: only `RATE_FINDING.md` reformatted (table column widths may adjust).

- [ ] **Step 5: Commit**

```bash
git add benchmark-framework/RATE_FINDING.md
git commit -m "docs(ramp): document BACKLOG vs THROUGHPUT verdict modes"
```

---

### Task 5: Testcontainers IT `THROUGHPUT` variant

Add an integration test proving `THROUGHPUT` converges to a sustainable rate at least as high as the `BACKLOG` path on the same broker (relative, machine-independent).

**Files:**
- Modify: `benchmark-framework/src/test/java/io/openmessaging/benchmark/ChopRateFinderKafkaIT.java`

**Interfaces:**
- Consumes: `RampVerdict.THROUGHPUT`, `Workload.rampMinThroughputRatio`; existing `discoveryWorkload()`, `writeKafkaDriverConfig(...)`, `KAFKA` container.

- [ ] **Step 1: Add a helper that runs discovery and returns the confirmed rate + sustainability**

Add to `ChopRateFinderKafkaIT`:

```java
    private double runDiscoveryAndAssertSustainable(Workload workload) throws Exception {
        File driverConfig = writeKafkaDriverConfig(KAFKA.getBootstrapServers());
        LocalWorker worker = new LocalWorker();
        worker.initializeDriver(driverConfig);
        TestResult result;
        try (WorkloadGenerator generator = new WorkloadGenerator("Kafka", workload, worker)) {
            result = generator.run();
        }
        double pubDelayAvgMs = result.aggregatedPublishDelayLatencyAvg / 1000.0;
        log.info(
                "verdict={} confirmedRate={} avgPublishDelay={} ms",
                workload.rampVerdict,
                result.rampVerification == null ? null : result.rampVerification.rate,
                String.format("%.1f", pubDelayAvgMs));
        assertThat(pubDelayAvgMs).isLessThan(500.0); // the confirmed rate must actually hold
        return result.rampVerification == null ? 0.0 : result.rampVerification.rate;
    }
```

- [ ] **Step 2: Write the failing THROUGHPUT test**

```java
    @Test
    void throughputVerdictFindsASustainableRateAtLeastAsHighAsBacklog() throws Exception {
        Workload backlog = discoveryWorkload();
        double backlogRate = runDiscoveryAndAssertSustainable(backlog);

        Workload throughput = discoveryWorkload();
        throughput.rampVerdict = RampVerdict.THROUGHPUT;
        throughput.rampMinThroughputRatio = 0.95;
        double throughputRate = runDiscoveryAndAssertSustainable(throughput);

        // THROUGHPUT must not under-report relative to the backlog-count path on the same broker.
        assertThat(throughputRate).isGreaterThanOrEqualTo(backlogRate * 0.9);
    }
```

- [ ] **Step 3: Run under the integration profile**

Run: `mvn -pl benchmark-framework verify -Pintegration-tests -Dit.test=ChopRateFinderKafkaIT -Dsurefire.skip=true -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true -Denforcer.skip=true`
Expected: PASS (Docker required). Both loads confirm sustainable rates; the throughput rate is ≥ 90% of the backlog rate. Note: this runs two discovery cycles — several minutes.

- [ ] **Step 4: Commit**

```bash
mvn -pl benchmark-framework spotless:apply
git add benchmark-framework/src/test/java/io/openmessaging/benchmark/ChopRateFinderKafkaIT.java
git commit -m "test(ramp): Testcontainers IT for THROUGHPUT verdict"
```

---

### Task 6: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Default build (IT skipped) — unit + static analysis**

Run: `mvn -pl benchmark-framework verify`
Expected: `BUILD SUCCESS`; `RampRateFinderTest` 20 tests; failsafe reports "Tests are skipped"; spotless/checkstyle/spotbugs clean.

- [ ] **Step 2: Integration profile — IT runs**

Run: `mvn -pl benchmark-framework verify -Pintegration-tests -Dit.test=ChopRateFinderKafkaIT -Dsurefire.skip=true`
Expected: `BUILD SUCCESS`; both IT methods pass.

- [ ] **Step 3: Multi-module sanity (optional, slower)**

Run: `mvn -q -pl benchmark-framework -am install -DskipTests`
Expected: `BUILD SUCCESS` — confirms the new enum/fields don't break dependents.

## Self-Review

- **Spec coverage:** `rampVerdict` enum + default BACKLOG (Task 1); `rampMinThroughputRatio` 0.95 (Tasks 1,3); predicate `published≥ratio·expected AND received≥ratio·published` at hold completion (Task 3 Step 4); no per-poll fast-fail for THROUGHPUT (`breachedNow=false`, Task 3 Step 4); machinery reused (Task 2 preserves it); no new Worker API (uses counters already passed to `poll`); back-compat default (Task 1 + Task 2 regression gate); tests unit+IT (Tasks 3,5); docs incl. AKS motivation + hold-length caveat (Task 4). All spec sections map to a task.
- **Placeholder scan:** the only prose-only step is Task 4 (a Markdown doc, content described precisely by section); every code step shows complete code.
- **Type consistency:** `RampVerdict.{BACKLOG,THROUGHPUT}`, `Workload.rampVerdict`/`rampMinThroughputRatio`, `holdClean()`, `breachedNow`, `holdExpected/holdPublished/holdReceived`, `previousTotalReceived`, `minThroughputRatio` used consistently across Tasks 1–3; `FakeThroughputSystem(double,double,long)` constructor matches all three call sites in Task 3 Step 1.
