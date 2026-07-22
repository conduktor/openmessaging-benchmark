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
 * Discovers the maximum sustainable producer rate: an exponential bracket phase finds a
 * known-good/known-bad pair around the knee, then a binary-chop phase holds and verifies each
 * candidate rate for a fixed window before accepting it, converging to a rate within a
 * configured tolerance.
 *
 * <p>Deliberately takes elapsed time and cumulative counters as explicit parameters (rather than
 * reading the clock itself) so it remains a fast, deterministic pure state machine to unit test;
 * the caller owns real sleeping/timing.
 */
class RampRateFinder {
    private static final long ONE_SECOND_IN_NANOS = SECONDS.toNanos(1);
    private static final int MAX_BRACKET_ITERATIONS = 20;

    enum Phase {
        BRACKET,
        CHOP,
        DONE
    }

    private final long publishBacklogLimit;
    private final long receiveBacklogLimit;
    private final long holdNanos;
    private final double convergenceTolerance;
    private final long maxDiscoveryNanos;

    @Getter(PACKAGE)
    private Phase phase = Phase.BRACKET;

    @Getter(PACKAGE)
    private Double lo;

    @Getter(PACKAGE)
    private Double hi;

    @Getter private double currentRate;

    @Getter private boolean nonMonotonic = false;

    private boolean confirming = false;
    private long previousTotalPublished = 0;
    private long elapsedHoldNanos = 0;
    private long totalElapsedNanos = 0;
    private int bracketIterations = 0;

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
        int holdSeconds =
                workload.rampHoldSeconds != null ? workload.rampHoldSeconds.intValue() : 30;
        this.holdNanos = SECONDS.toNanos(holdSeconds);
        this.convergenceTolerance =
                workload.rampConvergenceTolerance != null
                        ? workload.rampConvergenceTolerance.doubleValue()
                        : 0.05;
        int maxDiscoveryMinutes =
                workload.rampMaxDiscoveryMinutes != null
                        ? workload.rampMaxDiscoveryMinutes.intValue()
                        : 10;
        this.maxDiscoveryNanos = MINUTES.toNanos(maxDiscoveryMinutes);
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

        long expected = (long) ((currentRate / ONE_SECOND_IN_NANOS) * periodNanos);
        long published = totalPublished - previousTotalPublished;
        long receiveBacklog = totalPublished - totalReceived;
        long publishBacklog = expected - published;
        previousTotalPublished = totalPublished;

        boolean backlogExceeded =
                receiveBacklog > receiveBacklogLimit || publishBacklog > publishBacklogLimit;

        return phase == Phase.BRACKET
                ? pollBracket(backlogExceeded)
                : pollChop(backlogExceeded, periodNanos);
    }

    private boolean pollBracket(boolean backlogExceeded) {
        recordVerdict(currentRate, !backlogExceeded);

        if (backlogExceeded) {
            hi = currentRate;
        } else {
            lo = currentRate;
        }

        if (lo != null && hi != null) {
            phase = Phase.CHOP;
            elapsedHoldNanos = 0;
            currentRate = (lo + hi) / 2.0;
            return false;
        }

        bracketIterations++;
        if (bracketIterations >= MAX_BRACKET_ITERATIONS) {
            finishWithBestKnown();
            return true;
        }

        currentRate = backlogExceeded ? currentRate / 2.0 : currentRate * 2.0;
        return false;
    }

    private boolean pollChop(boolean backlogExceeded, long periodNanos) {
        if (backlogExceeded) {
            recordVerdict(currentRate, false);
            if (confirming) {
                // The converged rate didn't hold on re-check. Stop anyway (best effort) --
                // isNonMonotonic() is now true so the caller can warn about reproducibility.
                phase = Phase.DONE;
                return true;
            }
            hi = currentRate;
            elapsedHoldNanos = 0;
            currentRate = (lo + hi) / 2.0;
            return false;
        }

        elapsedHoldNanos += periodNanos;
        if (elapsedHoldNanos < holdNanos) {
            return false;
        }

        recordVerdict(currentRate, true);
        elapsedHoldNanos = 0;

        if (!confirming) {
            lo = currentRate;
        }

        if ((hi - lo) / lo <= convergenceTolerance) {
            if (!confirming) {
                // Require one more confirmation hold at the same rate before accepting it --
                // this is what makes isNonMonotonic() a real, testable signal rather than one
                // that binary chop's own strictly-nested bracket could never actually trigger.
                confirming = true;
                currentRate = lo;
                return false;
            }
            currentRate = lo;
            phase = Phase.DONE;
            return true;
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
