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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.worker.Worker;
import io.openmessaging.benchmark.worker.commands.ConsumerAssignment;
import io.openmessaging.benchmark.worker.commands.CountersStats;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import io.openmessaging.benchmark.worker.commands.PeriodStats;
import io.openmessaging.benchmark.worker.commands.ProducerWorkAssignment;
import io.openmessaging.benchmark.worker.commands.TopicsInfo;
import java.io.File;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Covers the guards around rate discovery that live in {@link WorkloadGenerator} rather than in the
 * finder state machine: rejecting configurations discovery cannot answer, and refusing to hand a
 * meaningless rate to the measurement window.
 */
class WorkloadGeneratorRampTest {

    @Test
    void rateDiscoveryWithoutSubscriptionsIsRejected() {
        // subscriptionsPerTopic is a primitive int, so an omitted -- or, since the YAML mapper has
        // FAIL_ON_UNKNOWN_PROPERTIES disabled, a misspelled -- field silently lands here as 0. With
        // no consumers there is no drain signal at all, so "sustainable" has no meaning and every
        // candidate rate fails; discovery must refuse rather than talk itself down to ~1 msg/s.
        Workload workload = discoveryWorkload();
        workload.subscriptionsPerTopic = 0;

        assertThatThrownBy(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("subscriptionsPerTopic");
    }

    @Test
    void rateDiscoveryWithoutConsumersPerSubscriptionIsRejected() {
        Workload workload = discoveryWorkload();
        workload.consumerPerSubscription = 0;

        assertThatThrownBy(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumerPerSubscription");
    }

    @Test
    void fixedRateWorkloadWithoutConsumersIsStillAllowed() {
        // Only *discovery* needs a drain signal. A produce-only run at a fixed rate is a legitimate
        // configuration and must not start failing because of the guard above.
        Workload workload = discoveryWorkload();
        workload.producerRate = 1000;
        workload.subscriptionsPerTopic = 0;

        assertThatCode(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .doesNotThrowAnyException();
    }

    @Test
    void impossibleThroughputRatioIsRejected() {
        // A ratio above 1 asks the producer to publish more than it was told to, which the rate
        // limiter's fixed virtual schedule makes impossible: every candidate fails and the search
        // halves to nothing. Cheaper to reject than to diagnose from a collapsed run.
        Workload workload = discoveryWorkload();
        workload.rampVerdict = RampVerdict.THROUGHPUT;
        workload.rampMinThroughputRatio = 1.5;

        assertThatThrownBy(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rampMinThroughputRatio");
    }

    @Test
    void convergenceToleranceOfOneOrMoreIsRejected() {
        // Confirmation holds run at lo x (1 - rampConvergenceTolerance), so a tolerance of 1 or
        // more drives the confirmed rate to zero or negative.
        Workload workload = discoveryWorkload();
        workload.rampConvergenceTolerance = 1.0;

        assertThatThrownBy(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rampConvergenceTolerance");
    }

    @Test
    void backlogFloorAboveCeilingIsRejected() {
        // Math.max(floor, min(rate x seconds, ceiling)) means the floor silently wins and the
        // ceiling is never applied -- the exact combination that let a runaway bracket confirm a
        // two-orders-of-magnitude-wrong rate.
        Workload workload = discoveryWorkload();
        workload.rampMaxBacklogSeconds = 0.1;
        workload.rampMaxBacklogFloor = 50_000L;
        workload.rampMaxBacklogCeiling = 1_000L;

        assertThatThrownBy(() -> new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rampMaxBacklogFloor");
    }

    @Test
    void discoveryThatNeverFindsASustainableRateFailsInsteadOfReportingNearZero() throws Exception {
        // Consumers never drain, so every candidate breaches and the bracket phase halves all the
        // way down. finishWithBestKnown() only assigns lo when one exists, so currentRate is left at
        // rampStartRate / 2^20 -- which LocalWorker then clamps to 1 msg/s and the measurement
        // window runs at for the whole test. That is the shape of the reproduced 0.9155 msg/s
        // incident, and it must be a hard failure instead.
        Workload workload = discoveryWorkload();
        try (WorkloadGenerator generator =
                new WorkloadGenerator("Kafka", workload, new StalledConsumerWorker())) {
            assertThatThrownBy(generator::run)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no candidate rate");
        }
    }

    private static Workload discoveryWorkload() {
        Workload workload = new Workload();
        workload.name = "ramp-guard-test";
        workload.topics = 1;
        workload.partitionsPerTopic = 1;
        workload.messageSize = 100;
        // Build the payload in memory so no payload/*.data file is needed.
        workload.useRandomizedPayloads = true;
        workload.randomBytesRatio = 1.0;
        workload.randomizedPayloadPoolSize = 1;
        workload.subscriptionsPerTopic = 1;
        workload.consumerPerSubscription = 1;
        workload.producersPerTopic = 1;

        workload.producerRate = 0; // discover the rate
        workload.rampAlgorithm = RampAlgorithm.CHOP;
        workload.rampStartRate = 1000;
        workload.rampReceiveBacklogLimit = 1L; // any lag at all is a breach
        // Zero-length periods and holds so the bracket phase exhausts its 20 iterations instantly.
        workload.rampBracketPeriodSeconds = 0;
        workload.rampSettleSeconds = 0;
        workload.rampBracketHoldSeconds = 0;
        workload.rampHoldSeconds = 0;
        workload.warmupDurationMinutes = 0;
        workload.testDurationMinutes = 1;
        return workload;
    }

    /**
     * A worker whose consumers deliver just enough to pass the topic-readiness probe and then stop,
     * so publishes accumulate as unbounded receive backlog and no candidate rate can ever hold.
     * Collecting stats is rejected outright: reaching the measurement window at all means discovery
     * handed on a rate it should have refused.
     */
    private static final class StalledConsumerWorker implements Worker {
        private long totalSent;

        @Override
        public CountersStats getCountersStats() {
            totalSent += 1_000;
            CountersStats stats = new CountersStats();
            stats.messagesSent = totalSent;
            stats.messagesReceived = 1; // satisfies ensureTopicsAreReady, then never advances
            return stats;
        }

        @Override
        public List<String> createTopics(TopicsInfo topicsInfo) {
            return List.of("topic-0");
        }

        @Override
        public PeriodStats getPeriodStats() {
            throw new UnsupportedOperationException(
                    "measurement window must not be reached when discovery found no rate");
        }

        @Override
        public CumulativeLatencies getCumulativeLatencies() {
            throw new UnsupportedOperationException(
                    "measurement window must not be reached when discovery found no rate");
        }

        @Override
        public String id() {
            return "stalled-consumer-worker";
        }

        @Override
        public void initializeDriver(File configurationFile) {}

        @Override
        public void createProducers(List<String> topics) {}

        @Override
        public void createConsumers(ConsumerAssignment consumerAssignment) {}

        @Override
        public void probeProducers() {}

        @Override
        public void startLoad(ProducerWorkAssignment producerWorkAssignment) {}

        @Override
        public void adjustPublishRate(double publishRate) {}

        @Override
        public void pauseConsumers() {}

        @Override
        public void resumeConsumers() {}

        @Override
        public void resetStats() {}

        @Override
        public void stopAll() {}

        @Override
        public void close() {}
    }
}
