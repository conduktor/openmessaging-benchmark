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
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RampRateFinderTest {

    private static Workload workload() {
        Workload workload = new Workload();
        workload.rampPublishBacklogLimit = 100L;
        workload.rampReceiveBacklogLimit = 100L;
        return workload;
    }

    @Test
    void bracketDoublesUntilBacklogThenTransitionsToChop() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(4500);
        long periodNanos = SECONDS.toNanos(3);

        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.BRACKET);

        // 1000 -> clean, 2000 -> clean, 4000 -> clean, 8000 -> exceeds capacity
        for (int i = 0; i < 4; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
        assertThat(finder.getLo()).isEqualTo(4000.0);
        assertThat(finder.getHi()).isEqualTo(8000.0);
        assertThat(finder.getCurrentRate()).isEqualTo(6000.0);
    }

    @Test
    void chopConvergesWithinToleranceAndReportsLo() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 6;
        workload.rampConvergenceTolerance = 0.05;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(4500);
        long periodNanos = SECONDS.toNanos(3);

        boolean done = false;
        for (int i = 0; i < 100 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.isNonMonotonic()).isFalse();
        // The backlog limit tolerates a small overshoot past true capacity before it's detected,
        // so the discovered rate lands just above 4500, not below it -- assert closeness instead.
        assertThat(Math.abs(4500.0 - finder.getCurrentRate()) / 4500.0).isLessThan(0.05);
    }

    @Test
    void holdFailsPartwayExitsEarlyWithoutWaitingOutTheFullHold() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 30; // long hold -- a single failing poll must not wait this out
        workload.rampConvergenceTolerance = 0.05;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        // Bracket: 1000 clean, 2000 exceeds -> CHOP with lo=1000, hi=2000, mid=1500
        finder.poll(periodNanos, 3000, 3000);
        finder.poll(periodNanos, 3000 + 3000, 3000 + 3000);
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
        assertThat(finder.getCurrentRate()).isEqualTo(1500.0);

        // First hold poll at 1500 is clean (published keeps up with expected)
        finder.poll(periodNanos, 6000 + 4500, 6000 + 4500);
        assertThat(finder.getHi()).isEqualTo(2000.0); // unchanged so far

        // Second hold poll at 1500 shows a backlog breach -- must fail immediately, not after 30s
        boolean done = finder.poll(periodNanos, 10500 + 1000, 10500 + 1000);

        assertThat(done).isFalse();
        assertThat(finder.getHi()).isEqualTo(1500.0);
        assertThat(finder.getCurrentRate()).isEqualTo((1000.0 + 1500.0) / 2.0);
    }

    @Test
    void nonMonotonicVerdictIsFlaggedWhenConfirmationHoldContradictsThePriorPass() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 3; // one poll completes a hold
        workload.rampConvergenceTolerance = 0.5; // converge quickly for this test
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        // Bracket: 1000 clean, 2000 exceeds -> CHOP, lo=1000, hi=2000, mid=1500
        finder.poll(periodNanos, 3000, 3000);
        finder.poll(periodNanos, 6000, 6000);
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);

        // Hold at 1500 passes cleanly -- within tolerance (500/1500 = 0.33 <= 0.5), so this
        // triggers the one-more confirmation hold at the same rate rather than DONE yet.
        boolean done = finder.poll(periodNanos, 6000 + 4500, 6000 + 4500);
        assertThat(done).isFalse();
        assertThat(finder.getCurrentRate()).isEqualTo(1500.0);

        // Confirmation hold at the same rate (1500) now shows a backlog breach -- a direct
        // contradiction of the pass just recorded at the same rate.
        done = finder.poll(periodNanos, 10500 + 2000, 10500 + 2000);

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.isNonMonotonic()).isTrue();
    }

    @Test
    void safetyCapForcesCompletionWithBestKnownRate() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampMaxDiscoveryMinutes = 1;
        RampRateFinder finder = new RampRateFinder(workload);

        // First poll: clean, establishes lo=1000, well within the 60s cap.
        boolean done = finder.poll(SECONDS.toNanos(30), 30000, 30000);
        assertThat(done).isFalse();
        assertThat(finder.getLo()).isEqualTo(1000.0);

        // Second poll pushes total elapsed time past the 60s cap -- forces completion
        // regardless of what the counters say.
        done = finder.poll(SECONDS.toNanos(40), 30000, 30000);

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.getCurrentRate()).isEqualTo(1000.0);
    }

    /** A simple producer-limited system: throughput is capped at {@code capacity} msgs/sec. */
    private static final class FakeSystem {
        private final double capacity;
        long totalPublished = 0;
        long totalReceived = 0;

        FakeSystem(double capacity) {
            this.capacity = capacity;
        }

        void advance(double rate, long periodNanos) {
            double periodSeconds = periodNanos / 1e9;
            long sent = (long) (Math.min(rate, capacity) * periodSeconds);
            totalPublished += sent;
            totalReceived += sent;
        }
    }
}
