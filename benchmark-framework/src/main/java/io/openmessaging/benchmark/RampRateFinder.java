/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.openmessaging.benchmark.utils.Env;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Discovers the maximum sustainable producer rate: after an initial settle period, an exponential
 * bracket phase finds a known-good/known-bad pair around the knee, then a binary-chop phase narrows
 * within it -- both phases hold every candidate for a configured duration before trusting it clean,
 * converging to a rate within a configured tolerance.
 *
 * <p>Deliberately takes elapsed time and cumulative counters as explicit parameters (rather than
 * reading the clock itself) so it remains a fast, deterministic pure state machine to unit test;
 * the caller owns real sleeping/timing.
 */
@Slf4j
class RampRateFinder {
    private static final long ONE_SECOND_IN_NANOS = SECONDS.toNanos(1);
    private static final int MAX_BRACKET_ITERATIONS = 20;

    // Used only as a last resort when a failed confirmation has no prior passing rate below it
    // to fall back on (history should always have one via the bracket phase, but this keeps the
    // reopened search bounded even in that edge case).
    private static final double CONFIRMATION_BACKOFF_FACTOR = 0.9;

    // Recovery runs at this fraction of lo. lo means "keeps up", not "has spare capacity", so
    // draining
    // at lo itself leaves the queue shrinking at (capacity - lo) -- nearly nothing. Two AKS
    // recoveries
    // ran their full 180s cap and gave up with the producer still 10 and 24 seconds behind, draining
    // at
    // ~1.16M against a ~1.17M ceiling. Half leaves real headroom, and since nothing is evaluated
    // during
    // recovery and it ends as soon as both sides are caught up, running slower costs nothing.
    private static final double DRAIN_HEADROOM_FACTOR = 0.5;

    enum Phase {
        BRACKET,
        CHOP,
        DONE
    }

    private final long publishBacklogLimit;
    private final long receiveBacklogLimit;
    private final Double maxBacklogSeconds;
    private final long maxBacklogFloor;
    private final long maxBacklogCeiling;
    private final long settleNanos;
    private final long bracketHoldNanos;
    private final long holdNanos;
    private final long drainNanos;
    private final double convergenceTolerance;
    private final long maxDiscoveryNanos;
    private final int requiredConfirmationHolds;
    private final RampVerdict verdict;
    private final double minThroughputRatio;
    private final boolean seedFromAchievedRate;
    private final int requiredBreachPolls;

    // Every publish is delivered once per subscription, so the consumers must move
    // subscriptions x published to keep up. Comparing raw counters would make the drain look
    // subscriptions-times healthier than it is. WorkloadGenerator applies the same factor wherever
    // it reports backlog.
    private final long subscriptions;

    @Getter(PACKAGE)
    private Phase phase = Phase.BRACKET;

    @Getter(PACKAGE)
    private Double lo;

    @Getter(PACKAGE)
    private Double hi;

    @Getter private double currentRate;

    @Getter private boolean nonMonotonic = false;

    @Getter(PACKAGE)
    private boolean confirming = false;

    // True only when discovery ended via a genuine two-hold confirm -- never set by the safety
    // cap or by a rejected confirmation hold, so callers can tell those apart from the outside.
    @Getter(PACKAGE)
    private boolean confirmed = false;

    @Getter(PACKAGE)
    private boolean settled = false;

    // Set only when the time budget forced completion, so a truncated discovery can be told apart
    // from one that ran to a genuine confirm -- otherwise both just return a rate.
    @Getter(PACKAGE)
    private boolean safetyCapped = false;

    // True while recovering from a failed candidate's overshoot. Nothing is evaluated while set.
    @Getter(PACKAGE)
    private boolean draining = false;

    private long previousTotalPublished = 0;
    private long previousTotalReceived = 0;
    private long elapsedSettleNanos = 0;
    private long elapsedHoldNanos = 0;
    private long totalElapsedNanos = 0;
    private int bracketIterations = 0;
    private int confirmationHoldsPassed = 0;
    private long holdExpected = 0;
    private long holdPublished = 0;
    private long holdReceived = 0;
    private long holdElapsedNanos = 0;
    private long elapsedDrainNanos = 0;
    private double pendingRate = 0;

    // The hold's publish volume split at the midpoint of its target length, so holdClean() can judge
    // the trend across a hold and not only its total.
    private long firstHalfPublished = 0;
    private long firstHalfNanos = 0;
    private long secondHalfPublished = 0;
    private long secondHalfNanos = 0;

    // Kept for the verdict log line. The level at the deciding poll and the deepest the hold ever got
    // answer different questions: a hold can end with a clear backlog having been badly behind in the
    // middle, which is what a knee looks like from the outside.
    private long lastReceiveBacklog = 0;
    private long holdPeakReceiveBacklog = 0;

    // Consecutive breaching polls within the current hold. Reset by any clean poll.
    private int consecutiveBreaches = 0;

    private final List<RateVerdict> history = new ArrayList<>();

    RampRateFinder(Workload workload) {
        this.currentRate =
                workload.rampStartRate != null ? workload.rampStartRate.doubleValue() : 10000.0;
        this.publishBacklogLimit =
                workload.rampPublishBacklogLimit != null
                        ? workload.rampPublishBacklogLimit.longValue()
                        : Env.getLong("PUBLISH_BACKLOG_LIMIT", 1_000);
        this.receiveBacklogLimit =
                workload.rampReceiveBacklogLimit != null
                        ? workload.rampReceiveBacklogLimit.longValue()
                        : Env.getLong("RECEIVE_BACKLOG_LIMIT", 1_000);
        this.maxBacklogSeconds = workload.rampMaxBacklogSeconds;
        this.maxBacklogFloor =
                workload.rampMaxBacklogFloor != null ? workload.rampMaxBacklogFloor.longValue() : 1_000L;
        // 500,000 rather than the original 100,000. At 100,000 the ceiling silently overrode any
        // rampMaxBacklogSeconds above ~200,000 msg/s, turning the rate-scaled limit back into the fixed
        // count it exists to replace: an AKS run asking for 0.5s at 589,000 msg/s got 0.17s worth, and
        // that is what tripped its one false failure. 500,000 still catches the runaway the ceiling was
        // added for -- a candidate 2x past capacity accrues far more than 500,000 messages of shortfall
        // per poll at these rates -- while letting 0.5s mean 0.5s up to 1,000,000 msg/s.
        this.maxBacklogCeiling =
                workload.rampMaxBacklogCeiling != null
                        ? workload.rampMaxBacklogCeiling.longValue()
                        : 500_000L;
        int settleSeconds =
                workload.rampSettleSeconds != null ? workload.rampSettleSeconds.intValue() : 30;
        this.settleNanos = SECONDS.toNanos(settleSeconds);
        int holdSeconds = workload.rampHoldSeconds != null ? workload.rampHoldSeconds.intValue() : 180;
        this.holdNanos = SECONDS.toNanos(holdSeconds);
        // Bracket only has to *locate* the knee, and overload announces itself in a poll or two: an AKS
        // arm went from ~1,300 messages of backlog to ~74,800 in a single 3-second poll the moment a
        // candidate genuinely exceeded capacity. So a short probe is enough, and chop re-verifies lo at
        // full length before narrowing anything, which is what makes it safe. Measured on the AKS
        // geometry: 90s probes cost 1,494s of discovery, 20s cost 942s (-37%), converging on the
        // identical rate. Never longer than the chop hold, or the re-verification would not trigger.
        int bracketHoldSeconds =
                workload.rampBracketHoldSeconds != null
                        ? workload.rampBracketHoldSeconds.intValue()
                        : Math.min(20, holdSeconds);
        this.bracketHoldNanos = SECONDS.toNanos(bracketHoldSeconds);
        // Defaults to the resolved rampHoldSeconds, i.e. on. It is a cap and not a fixed wait --
        // recovery
        // ends as soon as the backlog is back inside the limit it is judged against, so it costs a
        // single
        // poll when there is nothing to drain -- and the AKS trial showed what it costs to leave off:
        // the
        // encrypt arm rejected eight consecutive candidates in 24 seconds, none of them measured, each
        // failing on the previous candidate's undrained overshoot rather than on its own rate. A hold's
        // worth of patience is the natural bound: a candidate needing longer than that to drain is not
        // one the search should be judging yet.
        int drainSeconds =
                workload.rampDrainSeconds != null ? workload.rampDrainSeconds.intValue() : holdSeconds;
        this.drainNanos = SECONDS.toNanos(drainSeconds);
        this.convergenceTolerance =
                workload.rampConvergenceTolerance != null
                        ? workload.rampConvergenceTolerance.doubleValue()
                        : 0.05;
        // Coupled to holdSeconds *and* drainSeconds. Under THROUGHPUT every candidate costs a full
        // hold,
        // and a search from the default start rate to a 7-figure ceiling runs ~15 holds (8 bracket, ~5
        // chop, 1 confirm) plus settle -- ~45 minutes at the 180s default hold. Add the drain: it is a
        // cap rather than a wait (observed 2-30s in practice), but a search with ~6 failures could in
        // the
        // worst case spend another 6 x 180s waiting. 75 minutes covers both without truncating.
        // RampRateFinderTest pins the budget against the hold so they cannot drift apart again.
        int maxDiscoveryMinutes =
                workload.rampMaxDiscoveryMinutes != null ? workload.rampMaxDiscoveryMinutes.intValue() : 45;
        this.maxDiscoveryNanos = MINUTES.toNanos(maxDiscoveryMinutes);
        this.requiredConfirmationHolds =
                workload.rampConfirmationHolds != null ? workload.rampConfirmationHolds.intValue() : 1;
        this.verdict = workload.rampVerdict != null ? workload.rampVerdict : RampVerdict.BACKLOG;
        this.minThroughputRatio =
                workload.rampMinThroughputRatio != null
                        ? workload.rampMinThroughputRatio.doubleValue()
                        : 0.95;
        // On by default: ~12% less discovery time with no measured accuracy cost, and it fired
        // correctly on every AKS arm whose failures were producer-side.
        this.seedFromAchievedRate =
                workload.rampSeedFromAchievedRate == null || workload.rampSeedFromAchievedRate;
        this.requiredBreachPolls =
                workload.rampBreachPolls != null ? Math.max(1, workload.rampBreachPolls.intValue()) : 2;
        this.subscriptions = workload.subscriptionsPerTopic;
    }

    // Advances the state machine given the latest period's counters. Returns true when done.
    boolean poll(long periodNanos, long totalPublished, long totalReceived) {
        return poll(periodNanos, totalPublished, totalReceived, 0L);
    }

    // As above, plus this period's p99 publish delay in microseconds -- how far behind its own
    // schedule
    // the producer is. Diagnostic only as far as the verdict goes; it is used solely to decide when
    // recovery is complete, because a producer's own send buffer holds work that has not been
    // published
    // and so contributes nothing to receive backlog. 0 means "not supplied" and reads as caught up.
    boolean poll(
            long periodNanos, long totalPublished, long totalReceived, long publishDelayP99Micros) {
        if (phase == Phase.DONE) {
            return true;
        }

        totalElapsedNanos += periodNanos;
        if (totalElapsedNanos >= maxDiscoveryNanos) {
            safetyCapped = true;
            finishWithBestKnown();
            return true;
        }

        if (!settled) {
            elapsedSettleNanos += periodNanos;
            // Keep the published baseline current while ignoring backlog entirely, so whatever
            // happened during settling (consumer-group rebalance tail, producer connection
            // warm-up) can never be mistaken for the candidate rate being unsustainable.
            previousTotalPublished = totalPublished;
            previousTotalReceived = totalReceived;
            if (elapsedSettleNanos < settleNanos) {
                return false;
            }
            settled = true;
            return false;
        }

        if (draining) {
            return pollDrain(periodNanos, totalPublished, totalReceived, publishDelayP99Micros);
        }

        long expected = (long) ((currentRate / ONE_SECOND_IN_NANOS) * periodNanos);
        long published = totalPublished - previousTotalPublished;
        long receiveBacklog = subscriptions * totalPublished - totalReceived;
        previousTotalPublished = totalPublished;

        long received = totalReceived - previousTotalReceived;
        previousTotalReceived = totalReceived;

        // Nothing acknowledged is an absence of data, not a measurement of capacity. Every counter and
        // histogram this finder sees is populated on ack -- WorkerStats.recordProducerSuccess
        // increments
        // messagesSent and records both latency histograms at the same moment -- so an acknowledgement
        // stall zeroes all of them at once while the messages are still in flight, unacknowledged. That
        // says nothing about whether the rate is too high.
        //
        // An AKS run mistook exactly this for a breach: two zero-ack polls failed a re-verification of
        // a
        // rate that had just held cleanly, and the search converged on 9,203 msg/s against a real
        // ceiling
        // near 1,100,000. The deferred acks arrived on the very next poll -- 41,278 msg/s against a
        // 5,000
        // target, p99 publish latency 10.7 seconds -- so nothing had been lost, only deferred. A
        // consecutive-breach requirement is no defence here, because consecutiveness is trivially true
        // when every poll in the window reads zero.
        //
        // So do not judge the candidate on it, and restart the hold rather than resuming a window that
        // was interrupted partway through: a hold has to be a continuous observation to mean what it
        // claims. If the stall never clears, the discovery budget caps and no rampVerification is
        // attached, which is the honest outcome for an outage.
        if (published == 0 && expected > 0) {
            log.info(
                    "FINDER-STALL no acknowledgements in a {}ms poll at {} msg/s (expected {}); not"
                            + " judged, hold restarted",
                    NANOSECONDS.toMillis(periodNanos),
                    currentRate,
                    expected);
            elapsedHoldNanos = 0;
            consecutiveBreaches = 0;
            return false;
        }

        // A fresh hold starts whenever elapsedHoldNanos was reset to 0 by the previous poll's
        // transition (or after settle). Reset the per-hold accumulators at that first poll.
        if (elapsedHoldNanos == 0) {
            holdExpected = 0;
            holdPublished = 0;
            holdReceived = 0;
            holdElapsedNanos = 0;
            firstHalfPublished = 0;
            firstHalfNanos = 0;
            secondHalfPublished = 0;
            secondHalfNanos = 0;
            holdPeakReceiveBacklog = 0;
            consecutiveBreaches = 0;
        }
        lastReceiveBacklog = receiveBacklog;
        holdPeakReceiveBacklog = Math.max(holdPeakReceiveBacklog, receiveBacklog);
        holdExpected += expected;
        holdPublished += published;
        holdReceived += received;
        // Tracked separately from elapsedHoldNanos, which the bracket path zeroes before recording
        // its verdict -- this one stays valid for as long as the hold's totals do.
        holdElapsedNanos += periodNanos;

        // Assign this period to a half of the hold. The midpoint comes from the *target* hold length,
        // which is known up front, so each period can be filed as it arrives rather than buffering
        // every poll of the hold and splitting at the end. A period belongs to the first half when it
        // started before the midpoint.
        long targetHoldNanos = phase == Phase.BRACKET ? bracketHoldNanos : holdNanos;
        if (holdElapsedNanos - periodNanos < targetHoldNanos / 2) {
            firstHalfPublished += published;
            firstHalfNanos += periodNanos;
        } else {
            secondHalfPublished += published;
            secondHalfNanos += periodNanos;
        }

        boolean breachedNow = backlogBreached(receiveBacklog);

        // Debounce the fast-fail. A single poll is one sample, and condemning a candidate on one sample
        // is a coin toss at the boundary -- worse, hi never reopens, so the mistake is permanent. On
        // AKS
        // a 640,000 msg/s candidate ran seven consecutive healthy polls and then dipped for one, 10%
        // past the limit; that lone sample capped the whole search 7% low, on a hold whose own
        // aggregate
        // was 99.13% of target. Genuine overload does not look like that: the same trial measured
        // backlog jumping from ~1,300 to ~74,800 in a single poll and then staying elevated for 12-15s.
        // So requiring consecutive breaches costs one poll against a real failure and rejects a
        // transient outright. Counts reset on any clean poll, so only a run of breaches trips it.
        consecutiveBreaches = breachedNow ? consecutiveBreaches + 1 : 0;
        boolean breached = consecutiveBreaches >= requiredBreachPolls;

        return phase == Phase.BRACKET
                ? pollBracket(breached, periodNanos)
                : pollChop(breached, periodNanos);
    }

    // Whether this poll's evidence condemns the candidate. Under THROUGHPUT there is no per-poll
    // verdict at all -- it is decided once from the hold's aggregates, see holdClean.
    //
    // The two sides are checked differently, and deliberately so. Receive backlog is a *level*: it is
    // cumulative, so comparing it against rate x rampMaxBacklogSeconds reads as "the consumers are at
    // most this many seconds behind", which is what that setting is meant to say. Publish shortfall
    // is
    // a *flow*, and comparing this poll's shortfall against the same limit said something quite
    // different -- at a 1s poll, 0.5 permitted a 50% shortfall every poll indefinitely, because
    // nothing
    // accumulated between polls. So the producer side is judged as a fraction of what was asked for,
    // over the hold so far, against rampMinThroughputRatio. That is dimensionless, needs no per-rate
    // tuning, and fast-fails a gross shortfall on the first poll while ignoring a small one.
    private boolean backlogBreached(long receiveBacklog) {
        if (verdict == RampVerdict.THROUGHPUT) {
            return false;
        }
        if (holdPublished < minThroughputRatio * holdExpected) {
            return true;
        }
        if (maxBacklogSeconds != null) {
            // Clamped at both ends. maxBacklogCeiling stops the tolerance growing unbounded as bracket's
            // exponential doubling runs past the real ceiling; maxBacklogFloor stops the opposite, where
            // a single early false failure halves the rate and, in the same stroke, halves the tolerance
            // -- the wrong direction for a recovery mechanism -- letting one bad reading cascade all the
            // way down to a near-zero "confirmed" rate. Both were real, reproduced incidents.
            return receiveBacklog > receiveBacklogLimitFor(currentRate);
        }
        return receiveBacklog > receiveBacklogLimit;
    }

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
            // When bracket ran cheaper than chop, lo was certified with less rigor than every candidate
            // chop is about to judge -- and lo only ever moves upward, so an over-confirmed lo can never
            // be undone later. Re-run it at full length before narrowing anything. That is what makes a
            // short rampBracketHoldSeconds safe: the probe locates the bracket, the full hold certifies
            // it. Bracket's early doublings are otherwise pure overhead on a fast cluster (an AKS run
            // spent 6 x 89s climbing to 320k with backlog flat the whole way, learning nothing).
            if (bracketHoldNanos < holdNanos) {
                return failed ? beginDrain(lo) : setRate(lo);
            }
            if (failed) {
                return beginDrain(nextAfterFailure());
            }
            return setRate((lo + hi) / 2.0);
        }

        bracketIterations++;
        if (bracketIterations >= MAX_BRACKET_ITERATIONS) {
            finishWithBestKnown();
            return true;
        }

        double next = failed ? currentRate / 2.0 : currentRate * 2.0;
        return failed ? beginDrain(next) : setRate(next);
    }

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
                // The candidate didn't hold on re-check, so it's not actually safe -- reopen the
                // search instead of handing an unverified rate to the caller. Tighten hi to the
                // rate that just failed, and fall back to the highest rate we've already seen
                // pass below it (never a blind guess) as the new lo.
                hi = currentRate;
                lo = bestKnownPassBelow(hi);
                confirming = false;
                confirmationHoldsPassed = 0;
                return beginDrain(nextAfterFailure());
            }
            hi = currentRate;
            // A candidate at or below lo has failed -- which is exactly what re-verifying a cheaply
            // probed lo is meant to catch. lo and hi would now collide, and bisecting a zero-width
            // bracket retests the same rate forever, so drop back to the highest rate history records
            // as passing strictly below it.
            if (lo >= hi) {
                lo = bestKnownPassBelow(hi);
            }
            return beginDrain(nextAfterFailure());
        }

        recordVerdict(currentRate, true);
        elapsedHoldNanos = 0;
        if (!confirming) {
            lo = currentRate;
        }

        if ((hi - lo) / lo <= convergenceTolerance) {
            if (!confirming) {
                // Require requiredConfirmationHolds more consecutive clean holds before accepting
                // -- but hold them at confirmRate(), not at lo. Chop converges to within
                // convergenceTolerance of a *failing* rate, so lo sits right at the knee and
                // re-passing it is close to a coin flip; that flip then latches nonMonotonic and
                // withholds the whole result. Confirming just below the knee makes the re-check
                // meaningful again, and means the rate we report is one that was actually held.
                confirming = true;
                confirmationHoldsPassed = 0;
                currentRate = confirmRate();
                return false;
            }
            confirmationHoldsPassed++;
            if (confirmationHoldsPassed >= requiredConfirmationHolds) {
                currentRate = confirmRate();
                confirmed = true;
                phase = Phase.DONE;
                return true;
            }
            currentRate = confirmRate();
            return false;
        }

        confirming = false;
        currentRate = (lo + hi) / 2.0;
        return false;
    }

    // The rate actually held and reported: the converged lo, less a convergenceTolerance-wide
    // safety margin. lo is the highest rate observed to pass, but chop only knows the knee to
    // within convergenceTolerance, so lo may sit fractionally above the real ceiling -- and under
    // THROUGHPUT it provably can, since that gate accepts any rate up to capacity / ratio. Backing
    // off by the same width the search is uncertain over keeps the reported number on the safe side.
    // The receive-side backlog the current mode tolerates at a given rate. Shared by the BACKLOG
    // verdict and by the recovery period's "has it drained yet" check, so the two agree on what
    // "clear" means rather than the drain inventing its own threshold.
    private double receiveBacklogLimitFor(double rate) {
        if (maxBacklogSeconds == null) {
            return receiveBacklogLimit;
        }
        return Math.max(
                maxBacklogFloor, Math.min(rate * maxBacklogSeconds, (double) maxBacklogCeiling));
    }

    // How far behind its own schedule the producer may still be and count as recovered.
    // rampMaxBacklogSeconds is already a tolerance expressed in seconds, so it is the natural value
    // when
    // set -- the same slack allowed on the consumer side, applied to the producer. One second
    // otherwise,
    // which is generous next to the sub-millisecond delays a healthy hold shows.
    private long producerCatchUpMicros() {
        return maxBacklogSeconds != null
                ? (long) (maxBacklogSeconds * 1_000_000L)
                : SECONDS.toMicros(1);
    }

    // Recovery after a failed candidate: run at the highest rate already known to hold and evaluate
    // nothing until both sides have caught up. Always returns false -- recovery never ends discovery.
    private boolean pollDrain(
            long periodNanos, long totalPublished, long totalReceived, long publishDelayP99Micros) {
        elapsedDrainNanos += periodNanos;
        // Re-baseline so the recovery period's traffic is never attributed to the next
        // candidate's hold, exactly as the settle phase does for start-up transients.
        previousTotalPublished = totalPublished;
        previousTotalReceived = totalReceived;
        long drainBacklog = subscriptions * totalPublished - totalReceived;
        // Both sides have to be caught up, not just the consumer. Work still sitting in the
        // producer's client buffer has not been published, so it contributes nothing to receive
        // backlog -- with a 64MB buffer.memory that is hundreds of thousands of messages the backlog
        // check cannot see. Every drain in the local Kafka run reported "recovery complete after 2s"
        // on that basis while publish delay was still seconds deep, and the next candidate then
        // measured the previous one's fallout as its own.
        boolean consumerCaughtUp = drainBacklog <= receiveBacklogLimitFor(currentRate);
        boolean producerCaughtUp = publishDelayP99Micros <= producerCatchUpMicros();
        boolean recovered = consumerCaughtUp && producerCaughtUp;
        boolean outOfPatience = elapsedDrainNanos >= drainNanos;
        if (recovered || outOfPatience) {
            log.info(
                    "FINDER-DRAIN recovery {} after {}s at {} msg/s (backlog {}, delayP99 {}ms,"
                            + " consumerCaughtUp={} producerCaughtUp={}); resuming at {} msg/s",
                    recovered ? "complete" : "capped",
                    SECONDS.convert(elapsedDrainNanos, NANOSECONDS),
                    currentRate,
                    drainBacklog,
                    publishDelayP99Micros / 1000,
                    consumerCaughtUp,
                    producerCaughtUp,
                    pendingRate);
            draining = false;
            elapsedDrainNanos = 0;
            currentRate = pendingRate;
        }
        return false;
    }

    private boolean setRate(double next) {
        currentRate = next;
        return false;
    }

    // The next candidate to try after a hold failed. Called only on failure paths, and only once hi
    // has been set to the rate that just failed -- so hi *is* that rate here, and lo is the highest
    // rate already known to hold.
    //
    // A failed hold has already measured the system: it was asked for hi and managed
    // lastHoldAchievedRate(), which is a direct estimate of capacity. Bisecting the bracket discards
    // that measurement and spends another full hold rediscovering it -- and under the THROUGHPUT
    // verdict there is no per-poll fast-fail, so every rediscovered hold costs its full length.
    private double nextAfterFailure() {
        double midpoint = (lo + hi) / 2.0;
        if (!seedFromAchievedRate) {
            return midpoint;
        }
        double achieved = lastHoldAchievedRate();
        // The estimate has to be meaningfully below the rate that failed to be an estimate at all. A
        // candidate that failed on consumer lag -- or on a broker absorbing the overshoot into its
        // buffers, which a real THROUGHPUT run did at achievedRatio 1.000 -- published everything it
        // was asked for, so its achieved rate is the target restated, not a measurement of capacity.
        // This guard is also what keeps the search geometric: consecutive seeded failures each pull hi
        // down by at least the tolerance, where an unguarded seed sitting just under hi would creep
        // down by an epsilon per hold -- slower than the bisection it replaced.
        boolean informative = achieved <= hi * (1.0 - convergenceTolerance);
        // ... and it has to beat the highest rate already observed to hold, or the bracket the search
        // has already paid for is the better evidence and re-testing covered ground wastes a hold.
        boolean aboveKnownGood = achieved > lo;
        if (!informative || !aboveKnownGood) {
            return midpoint;
        }
        log.info(
                "FINDER-SEED next candidate {} msg/s from the failed hold's achieved rate"
                        + " (bisecting [{}, {}] would have tried {})",
                achieved,
                lo,
                hi,
                midpoint);
        return achieved;
    }

    // Enter recovery after a failed candidate: run at the highest rate already observed to hold
    // (falling back to the queued candidate when nothing has passed yet, e.g. bracket halving down
    // from a start rate that was already too high) and evaluate nothing until the backlog clears.
    private boolean beginDrain(double next) {
        // No drain without a known-good rate to drain at. The premise is that running at lo makes
        // queues
        // actually shrink; the next candidate guarantees nothing, since it may itself be above
        // capacity.
        // On bracket's downward path lo is still null, and draining there is worse than skipping it --
        // the recovery test never passes, the whole cap is burned, and the halved candidate then begins
        // its hold carrying a larger backlog than if the search had simply moved on.
        if (drainNanos == 0 || lo == null) {
            return setRate(next);
        }
        pendingRate = next;
        currentRate = lo * DRAIN_HEADROOM_FACTOR;
        draining = true;
        elapsedDrainNanos = 0;
        return false;
    }

    private double confirmRate() {
        return lo * (1.0 - convergenceTolerance);
    }

    // Throughput actually achieved over the most recently completed hold, in msg/s. currentRate is
    // what was asked for; this is what the system delivered. After discovery ends these totals are
    // still those of the final confirmation hold, so this is the honest figure to report.
    double lastHoldAchievedRate() {
        return holdElapsedNanos == 0
                ? 0.0
                : holdPublished / (holdElapsedNanos / (double) ONE_SECOND_IN_NANOS);
    }

    private void finishWithBestKnown() {
        if (lo != null) {
            currentRate = lo;
        }
        phase = Phase.DONE;
    }

    // The highest rate ever recorded as a pass strictly below `ceiling` -- used to reopen the
    // search on a safe, already-observed footing after a failed confirmation, rather than a
    // blind guess. Falls back to a fixed backoff only if history somehow has no such rate yet.
    private double bestKnownPassBelow(double ceiling) {
        double best = 0;
        for (RateVerdict v : history) {
            if (v.passed && v.rate < ceiling && v.rate > best) {
                best = v.rate;
            }
        }
        return best > 0 ? best : ceiling * CONFIRMATION_BACKOFF_FACTOR;
    }

    // Whether the just-completed hold counts as clean. For BACKLOG this is always true: a real
    // breach fast-fails per-poll before the hold ever completes, so reaching completion means clean.
    // For THROUGHPUT, breachedNow never fast-fails (see poll()), so this is where the verdict is
    // actually decided: the producer must have kept up with the candidate rate, and the consumer
    // must have kept up with (drained) what the producer actually published.
    private boolean holdClean() {
        if (verdict != RampVerdict.THROUGHPUT) {
            // Reaching completion means no run of breaches was ever long enough to fast-fail. But a
            // hold that ends while still breaching has not recovered, and without this a hold shorter
            // than rampBreachPolls polls could never fail at all -- the debounce would swallow its only
            // sample. So the tolerance is for breaches the candidate *recovered* from, not for one that
            // is still in progress when the clock runs out.
            return consecutiveBreaches == 0;
        }
        boolean producerKeepsUp = holdPublished >= minThroughputRatio * holdExpected;
        // Note this is already a *divergence* check rather than a level check, which is why there is no
        // separate "backlog is not growing" gate: if the receive backlog grew by dB while the hold
        // published P, the consumer moved subscriptions x P - dB, so requiring
        // holdReceived >= ratio x subscriptions x holdPublished is exactly requiring
        // dB <= (1 - ratio) x subscriptions x P. Consumer-side divergence is caught at any hold length.
        boolean consumerKeepsUp = holdReceived >= minThroughputRatio * subscriptions * holdPublished;
        return producerKeepsUp && consumerKeepsUp && throughputIsNotDeclining();
    }

    // Whether the hold's second half published as fast as its first. The aggregate ratio above
    // averages
    // over the whole hold, which dilutes a decline confined to the tail: a broker that absorbs the
    // overshoot into page cache, batching and socket buffers acks at the full target rate until those
    // buffers saturate, so a hold that saturates near its end still totals above the ratio and is
    // accepted. Comparing the halves leaves that decline undiluted.
    //
    // This narrows the window rather than closing it. A hold whose buffers absorb the overshoot for
    // its
    // entire length is flat in both halves and no in-hold statistic can tell it from a healthy one --
    // only a longer hold can. What this buys is roughly the sensitivity of a hold twice as long, for
    // no
    // extra wall-clock, because the signal is concentrated in half the window instead of spread over
    // all of it.
    private boolean throughputIsNotDeclining() {
        // A hold of a single period has no second half to compare against; abstain rather than guess.
        if (firstHalfNanos == 0 || secondHalfNanos == 0) {
            return true;
        }
        return secondHalfRate() >= minThroughputRatio * firstHalfRate();
    }

    private double firstHalfRate() {
        return firstHalfPublished / (firstHalfNanos / (double) ONE_SECOND_IN_NANOS);
    }

    private double secondHalfRate() {
        return secondHalfPublished / (secondHalfNanos / (double) ONE_SECOND_IN_NANOS);
    }

    // A one-line resolved-config summary. Rendered by the caller at discovery start so a run's log
    // shows the values actually in effect -- the ramp fields all default silently, and the YAML
    // mapper ignores unknown properties, so a misspelled field otherwise looks like it applied.
    String describeConfig() {
        return String.format(
                "verdict=%s subscriptions=%d startRate=%s settle=%ds bracketHold=%ds hold=%ds"
                        + " drainCap=%ds tolerance=%s seedFromAchievedRate=%s breachPolls=%d"
                        + " confirmHolds=%d budget=%dmin minThroughputRatio=%s"
                        + " backlogLimits=[publish=%d receive=%d maxBacklogSeconds=%s floor=%d"
                        + " ceiling=%d]",
                verdict,
                subscriptions,
                currentRate,
                SECONDS.convert(settleNanos, NANOSECONDS),
                SECONDS.convert(bracketHoldNanos, NANOSECONDS),
                SECONDS.convert(holdNanos, NANOSECONDS),
                SECONDS.convert(drainNanos, NANOSECONDS),
                convergenceTolerance,
                seedFromAchievedRate,
                requiredBreachPolls,
                requiredConfirmationHolds,
                MINUTES.convert(maxDiscoveryNanos, NANOSECONDS),
                minThroughputRatio,
                publishBacklogLimit,
                receiveBacklogLimit,
                maxBacklogSeconds,
                maxBacklogFloor,
                maxBacklogCeiling);
    }

    private static double ratio(long numerator, long denominator) {
        return denominator == 0 ? 0.0 : (double) numerator / denominator;
    }

    private void recordVerdict(double rate, boolean passed) {
        // Every completed hold funnels through here exactly once, so this is the single place that
        // can show why a candidate was judged the way it was. The AKS incidents in RATE_FINDING.md
        // were diagnosed as "hard to say without instrumenting the actual hold windows"; this is
        // that instrumentation. INFO (not debug) to match RateController's FINDER-TRACE, which
        // emits regardless of the active log4j2 config.
        log.info(
                "FINDER-HOLD phase={} rate={} verdict={} confirming={} expected={} published={}"
                        + " received={} backlog={} backlogPeak={} achievedRatio={} drainRatio={}"
                        + " trendRatio={} bracket=[{}, {}]",
                phase,
                rate,
                passed ? "clean" : "exceeded",
                confirming,
                holdExpected,
                holdPublished,
                holdReceived,
                lastReceiveBacklog,
                holdPeakReceiveBacklog,
                String.format("%.3f", ratio(holdPublished, holdExpected)),
                String.format("%.3f", ratio(holdReceived, subscriptions * holdPublished)),
                // Second half of the hold against its first. "n/a" when the hold was too short to
                // split, so a hold the trend gate abstained on is distinguishable from a flat one.
                firstHalfNanos == 0 || secondHalfNanos == 0 || firstHalfRate() == 0
                        ? "n/a"
                        : String.format("%.3f", secondHalfRate() / firstHalfRate()),
                lo,
                hi);

        // Only full-length holds can contradict each other. A bracket probe run at a shorter
        // rampBracketHoldSeconds is a cheaper measurement, not a weaker verdict on the same thing: a 1s
        // probe passing where a 3s hold fails is the probe being cheap, which is the whole premise of
        // probing cheaply. Latching nonMonotonic there would withhold the result of every run that used
        // a cheap bracket, making the option useless. When bracket and chop hold for the same length
        // (the default) every verdict is full rigor and this is exactly the old behaviour.
        boolean fullRigor = phase != Phase.BRACKET || bracketHoldNanos >= holdNanos;
        for (RateVerdict v : history) {
            if (!fullRigor || !v.fullRigor) {
                continue;
            }
            if (passed && !v.passed && rate >= v.rate) {
                nonMonotonic = true;
            }
            if (!passed && v.passed && rate <= v.rate) {
                nonMonotonic = true;
            }
        }
        history.add(new RateVerdict(rate, passed, fullRigor));
    }

    private static final class RateVerdict {
        final double rate;
        final boolean passed;
        // False for a bracket probe shorter than a chop hold -- see recordVerdict.
        final boolean fullRigor;

        RateVerdict(double rate, boolean passed, boolean fullRigor) {
            this.rate = rate;
            this.passed = passed;
            this.fullRigor = fullRigor;
        }
    }
}
