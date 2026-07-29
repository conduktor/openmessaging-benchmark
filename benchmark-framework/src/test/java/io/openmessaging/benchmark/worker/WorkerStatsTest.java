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
package io.openmessaging.benchmark.worker;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Test;

/**
 * Covers the two-population counter contract {@code resetStats()} has to respect: the per-period
 * counters that feed each measurement interval, and the cumulative totals that a concurrently
 * running backlog drain reads.
 */
class WorkerStatsTest {

    private static WorkerStats stats() {
        return new WorkerStats(NullStatsLogger.INSTANCE);
    }

    // recordProducerSuccess increments both the period counter and the cumulative total, so it stands
    // in for a real ack. recordMessageSent is deliberately not used: it touches only the total, and
    // calling both would double-count it.
    private static void publishAndConsume(WorkerStats stats, int messages) {
        long base = SECONDS.toNanos(1);
        for (int i = 0; i < messages; i++) {
            stats.recordProducerSuccess(100, base, base + 1_000, base + 2_000);
            stats.recordMessageReceived(100, 500);
        }
    }

    @Test
    void resettingBeforeAMeasurementWindowClearsThePeriodCounters() throws Exception {
        // The bug this pins. printAndCollectStats computes each interval's rate as
        // periodStats.messagesSent / (now - previousPoll), but messagesSent accumulates until something
        // calls toPeriodStats(). If nothing does so between startLoad() and the window's first poll --
        // which is exactly what an AIMD ramp does, since its control loop reads getCountersStats() --
        // then the first interval divides every message published during discovery by one 10-second
        // period. Observed at 15,439,670 msg/s against a steady-state 481,000 on a producerRate: 0 run.
        WorkerStats stats = stats();
        publishAndConsume(stats, 5_000); // stands in for a long discovery phase

        stats.resetPeriodCounters();

        assertThat(stats.toPeriodStats().messagesSent)
                .as("a measurement window must not inherit counts from whatever ran before it")
                .isZero();
        assertThat(stats.toPeriodStats().messagesReceived).isZero();
    }

    @Test
    void resettingPreservesTheCumulativeTotalsABacklogDrainDependsOn() throws Exception {
        // Why this is not simply reset(). buildAndDrainBacklog is launched *before* resetStats() and
        // runs
        // concurrently for the whole test, computing its remaining backlog from CountersStats -- i.e.
        // from totalMessagesSent/totalMessagesReceived. Clearing those mid-flight would make its
        // backlog
        // collapse to zero and let it decide the drain had finished. So the period counters and the
        // cumulative totals have to be resettable independently, which is what reset() does not offer.
        WorkerStats stats = stats();
        publishAndConsume(stats, 5_000);

        stats.resetPeriodCounters();

        assertThat(stats.toCountersStats().messagesSent)
                .as("cumulative totals survive, or a concurrent backlog drain loses its footing")
                .isEqualTo(5_000);
        assertThat(stats.toCountersStats().messagesReceived).isEqualTo(5_000);
    }

    @Test
    void periodCountersStillAccumulateNormallyAfterAReset() throws Exception {
        // Guards the obvious way to get this wrong: resetting something that stops counting afterwards.
        WorkerStats stats = stats();
        publishAndConsume(stats, 100);
        stats.resetPeriodCounters();

        publishAndConsume(stats, 42);

        assertThat(stats.toPeriodStats().messagesSent).isEqualTo(42);
        assertThat(stats.toCountersStats().messagesSent).isEqualTo(142);
    }
}
