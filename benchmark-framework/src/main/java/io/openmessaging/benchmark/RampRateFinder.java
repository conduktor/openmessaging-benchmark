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
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.openmessaging.benchmark.utils.Env;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

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
class RampRateFinder {
    private static final long ONE_SECOND_IN_NANOS = SECONDS.toNanos(1);
    private static final int MAX_BRACKET_ITERATIONS = 20;

    // Used only as a last resort when a failed confirmation has no prior passing rate below it
    // to fall back on (history should always have one via the bracket phase, but this keeps the
    // reopened search bounded even in that edge case).
    private static final double CONFIRMATION_BACKOFF_FACTOR = 0.9;

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
    private final double convergenceTolerance;
    private final long maxDiscoveryNanos;
    private final int requiredConfirmationHolds;
    private final RampVerdict verdict;

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

    private long previousTotalPublished = 0;
    private long elapsedSettleNanos = 0;
    private long elapsedHoldNanos = 0;
    private long totalElapsedNanos = 0;
    private int bracketIterations = 0;
    private int confirmationHoldsPassed = 0;

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
        this.maxBacklogCeiling =
                workload.rampMaxBacklogCeiling != null
                        ? workload.rampMaxBacklogCeiling.longValue()
                        : 100_000L;
        int settleSeconds =
                workload.rampSettleSeconds != null ? workload.rampSettleSeconds.intValue() : 30;
        this.settleNanos = SECONDS.toNanos(settleSeconds);
        int holdSeconds = workload.rampHoldSeconds != null ? workload.rampHoldSeconds.intValue() : 30;
        this.holdNanos = SECONDS.toNanos(holdSeconds);
        int bracketHoldSeconds =
                workload.rampBracketHoldSeconds != null
                        ? workload.rampBracketHoldSeconds.intValue()
                        : holdSeconds;
        this.bracketHoldNanos = SECONDS.toNanos(bracketHoldSeconds);
        this.convergenceTolerance =
                workload.rampConvergenceTolerance != null
                        ? workload.rampConvergenceTolerance.doubleValue()
                        : 0.05;
        int maxDiscoveryMinutes =
                workload.rampMaxDiscoveryMinutes != null ? workload.rampMaxDiscoveryMinutes.intValue() : 10;
        this.maxDiscoveryNanos = MINUTES.toNanos(maxDiscoveryMinutes);
        this.requiredConfirmationHolds =
                workload.rampConfirmationHolds != null ? workload.rampConfirmationHolds.intValue() : 1;
        this.verdict = workload.rampVerdict != null ? workload.rampVerdict : RampVerdict.BACKLOG;
    }

    // Advances the state machine given the latest period's counters. Returns true when done.
    boolean poll(long periodNanos, long totalPublished, long totalReceived) {
        if (phase == Phase.DONE) {
            return true;
        }

        totalElapsedNanos += periodNanos;
        if (totalElapsedNanos >= maxDiscoveryNanos) {
            finishWithBestKnown();
            return true;
        }

        if (!settled) {
            elapsedSettleNanos += periodNanos;
            // Keep the published baseline current while ignoring backlog entirely, so whatever
            // happened during settling (consumer-group rebalance tail, producer connection
            // warm-up) can never be mistaken for the candidate rate being unsustainable.
            previousTotalPublished = totalPublished;
            if (elapsedSettleNanos < settleNanos) {
                return false;
            }
            settled = true;
            return false;
        }

        long expected = (long) ((currentRate / ONE_SECOND_IN_NANOS) * periodNanos);
        long published = totalPublished - previousTotalPublished;
        long receiveBacklog = totalPublished - totalReceived;
        long publishBacklog = expected - published;
        previousTotalPublished = totalPublished;

        boolean breachedNow;
        if (maxBacklogSeconds != null) {
            // A limit that scales with the candidate rate, so the predicate is equally strict
            // at every rate tested during bracket's exponential range -- a fixed message count
            // is comparatively loose at high rates and comparatively tight at low ones. Clamped
            // at both ends: maxBacklogCeiling stops the tolerance growing unbounded as bracket's
            // exponential doubling runs away past the real ceiling (a real incident: at a
            // 1,000,000+ msg/s candidate, an uncapped 1.0s tolerance meant a million messages of
            // backlog still counted as "clean," and the search reported a two-orders-of-magnitude
            // wrong rate as confirmed). maxBacklogFloor stops the opposite: without it, a single
            // early false failure halves the rate and, in the same stroke, halves the tolerance --
            // the wrong direction for a recovery mechanism -- letting one bad reading cascade all
            // the way down to a near-zero "confirmed" rate (also a real incident, reproduced
            // deterministically twice on the same starting conditions).
            double limit =
                    Math.max(
                            maxBacklogFloor,
                            Math.min(currentRate * maxBacklogSeconds, (double) maxBacklogCeiling));
            breachedNow = receiveBacklog > limit || publishBacklog > limit;
        } else {
            breachedNow = receiveBacklog > receiveBacklogLimit || publishBacklog > publishBacklogLimit;
        }

        return phase == Phase.BRACKET
                ? pollBracket(breachedNow, periodNanos)
                : pollChop(breachedNow, periodNanos);
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
                // Require requiredConfirmationHolds more consecutive clean holds at the same
                // rate before accepting it -- this is what makes isNonMonotonic() a real,
                // testable signal rather than one that binary chop's own strictly-nested
                // bracket could never actually trigger.
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
    // THROUGHPUT overrides this in a later task.
    private boolean holdClean() {
        return true;
    }

    private void recordVerdict(double rate, boolean passed) {
        for (RateVerdict v : history) {
            if (passed && !v.passed && rate >= v.rate) {
                nonMonotonic = true;
            }
            if (!passed && v.passed && rate <= v.rate) {
                nonMonotonic = true;
            }
        }
        history.add(new RateVerdict(rate, passed));
    }

    private static final class RateVerdict {
        final double rate;
        final boolean passed;

        RateVerdict(double rate, boolean passed) {
            this.rate = rate;
            this.passed = passed;
        }
    }
}
