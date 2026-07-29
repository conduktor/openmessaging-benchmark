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

import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PACKAGE;

import io.openmessaging.benchmark.utils.Env;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RateController {
    private static final long ONE_SECOND_IN_NANOS = SECONDS.toNanos(1);
    private final long publishBacklogLimit;
    private final long receiveBacklogLimit;

    // Each publish is delivered once per subscription, so the consumers owe
    // subscriptions x published deliveries. WorkloadGenerator applies the same factor wherever it
    // reports backlog.
    private final long subscriptions;
    private final double minRampingFactor;
    private final double maxRampingFactor;
    private final double warmupRateFraction;

    @Getter(PACKAGE)
    private double rampingFactor;

    private long previousTotalPublished = 0;
    private long previousTotalReceived = 0;
    private boolean warmedUp = false;

    RateController() {
        this(null, null);
    }

    RateController(Long publishBacklogLimit, Long receiveBacklogLimit) {
        this(publishBacklogLimit, receiveBacklogLimit, 1);
    }

    RateController(Long publishBacklogLimit, Long receiveBacklogLimit, long subscriptions) {
        this.subscriptions = subscriptions;
        this.publishBacklogLimit =
                publishBacklogLimit != null
                        ? publishBacklogLimit
                        : Env.getLong("PUBLISH_BACKLOG_LIMIT", 1_000);
        this.receiveBacklogLimit =
                receiveBacklogLimit != null
                        ? receiveBacklogLimit
                        : Env.getLong("RECEIVE_BACKLOG_LIMIT", 1_000);
        minRampingFactor = Env.getDouble("MIN_RAMPING_FACTOR", 0.01);
        maxRampingFactor = Env.getDouble("MAX_RAMPING_FACTOR", 1);
        // Fraction of the offered rate the producer must reach before the controller starts
        // adjusting. Until then the rate is HELD (see nextRate) so a still-starting producer
        // isn't mistaken for a saturated one. 0 disables the guard (legacy behaviour).
        warmupRateFraction = Env.getDouble("WARMUP_RATE_FRACTION", 0.5);
        rampingFactor = maxRampingFactor;
    }

    double nextRate(double rate, long periodNanos, long totalPublished, long totalReceived) {
        long expected = (long) ((rate / ONE_SECOND_IN_NANOS) * periodNanos);
        long published = totalPublished - previousTotalPublished;
        long received = totalReceived - previousTotalReceived;

        previousTotalPublished = totalPublished;
        previousTotalReceived = totalReceived;

        // DIAGNOSTIC: info (not debug) so the finder trajectory emits regardless of the active
        // log4j2 config (the slf4j binding is log4j2; debug-level knobs didn't take). Only active
        // during a producerRate:0 finder, so it's quiet for fixed-rate sweeps/repeats.
        log.info(
                "FINDER-TRACE Current rate: {} -- Publish rate {} -- Receive Rate: {}",
                rate,
                rate(published, periodNanos),
                rate(received, periodNanos));

        // Startup guard: hold the offered rate until the producer has proven it can sustain it at
        // least once. A control window sampled while the producer is still starting sees
        // published << expected; treating that as saturation would crater the rate toward 0, from
        // which the multiplicative ramp (rate + rate*factor) cannot recover — the observed encrypt
        // finder collapse to ~1 msg/s. Once warmed, backoffs settle at the real achieved rate.
        if (!warmedUp) {
            if (published >= (long) (expected * warmupRateFraction)) {
                warmedUp = true;
            } else {
                return rate;
            }
        }

        long receiveBacklog = subscriptions * totalPublished - totalReceived;
        if (receiveBacklog > receiveBacklogLimit) {
            return nextRate(periodNanos, received, expected, receiveBacklog, "Receive");
        }

        long publishBacklog = expected - published;
        if (publishBacklog > publishBacklogLimit) {
            return nextRate(periodNanos, published, expected, publishBacklog, "Publish");
        }

        rampUp();

        return rate + (rate * rampingFactor);
    }

    private double nextRate(long periodNanos, long actual, long expected, long backlog, String type) {
        log.info("FINDER-TRACE {} backlog: {} -> backing off", type, backlog);
        rampDown();
        long nextExpected = Math.max(0, expected - backlog);
        double nextExpectedRate = rate(nextExpected, periodNanos);
        double actualRate = rate(actual, periodNanos);
        return Math.min(actualRate, nextExpectedRate);
    }

    private double rate(long count, long periodNanos) {
        return (count / (double) periodNanos) * ONE_SECOND_IN_NANOS;
    }

    private void rampUp() {
        rampingFactor = Math.min(maxRampingFactor, rampingFactor * 2);
    }

    private void rampDown() {
        rampingFactor = Math.max(minRampingFactor, rampingFactor / 2);
    }
}
