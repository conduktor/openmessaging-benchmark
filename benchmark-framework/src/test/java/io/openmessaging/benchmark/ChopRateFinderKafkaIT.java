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

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.worker.LocalWorker;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end integration test for the opt-in CHOP rate finder, driving the real {@link
 * WorkloadGenerator} discovery loop against a throwaway Kafka broker.
 *
 * <p>This is the first automated test in the repo that exercises a rate finder against an actual
 * broker rather than hand-fed counters. {@code RampRateFinderTest} proves the clamp
 * <em>arithmetic</em> deterministically (via a {@code FakeSystem}); this proves the clamps survive
 * the <em>integrated</em> path -- real producers/consumers, real backlog signals, real hold timing
 * -- which is exactly the layer where both AKS "Trial finding" incidents in {@code RATE_FINDING.md}
 * lived.
 *
 * <p>Skipped by default (failsafe {@code skipITs=true}) so PR CI stays fast and Docker-free. Run it
 * on demand with a Docker daemon available: {@code mvn -pl benchmark-framework verify
 * -Pintegration-tests}. Takes a few minutes -- discovery holds must be long enough that a bursting
 * broker cannot masquerade an unsustainable rate as clean.
 */
@Testcontainers
class ChopRateFinderKafkaIT {

    private static final Logger log = LoggerFactory.getLogger(ChopRateFinderKafkaIT.class);

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    /**
     * The claim CHOP makes that AIMD never did: the rate it confirms is one you can actually hold.
     * Drives real discovery against a real broker, then checks the measurement window that ran at the
     * confirmed rate.
     *
     * <p>Every assertion here exists because its absence let a real incident through. Asserting only
     * that publish delay stayed bounded -- the original check -- passes when {@code rampVerification}
     * is null, and passes when discovery collapsed to 1 msg/s, since a flatline has no publish delay
     * either. So the test could not fail for the incident it was written to catch.
     */
    @Test
    void backlogVerdictConfirmsARateItCanActuallyHold() throws Exception {
        Discovery discovery = runDiscovery(RampVerdict.BACKLOG);
        assertVerifiedAndSustainable(discovery);
    }

    /**
     * THROUGHPUT exists because BACKLOG under-reported the true sustainable rate by up to ~320x on a
     * healthy cluster (see RATE_FINDING.md). The assertion is deliberately *relative* -- "at least as
     * high as the count-based path on this same broker" -- so it means the same thing on a laptop and
     * in CI, where an absolute msg/s threshold would not.
     */
    @Test
    void throughputVerdictDoesNotUnderReportRelativeToBacklog() throws Exception {
        Discovery backlog = runDiscovery(RampVerdict.BACKLOG);
        Discovery throughput = runDiscovery(RampVerdict.THROUGHPUT);

        assertVerifiedAndSustainable(throughput);
        assertThat(throughput.confirmedRate)
                .as(
                        "THROUGHPUT (%s msg/s) must not under-report against BACKLOG (%s msg/s) on the"
                                + " same broker -- under-reporting is the whole reason it exists",
                        throughput.confirmedRate, backlog.confirmedRate)
                .isGreaterThanOrEqualTo(backlog.confirmedRate);
    }

    private static void assertVerifiedAndSustainable(Discovery discovery) {
        // A withheld rampVerification means discovery did not reach a genuine confirm. The original
        // test read confirmedRate only to build a failure message, so a null sailed through.
        assertThat(discovery.confirmedRate)
                .as("discovery must reach a genuine confirm and attach rampVerification")
                .isNotNull();

        // Guards against the collapse-to-1-msg/s failure mode, which satisfies every
        // "is it sustainable" check trivially. A single-broker container handles far more than this.
        assertThat(discovery.confirmedRate)
                .as("a plausible rate for a real broker, not a collapsed one")
                .isGreaterThan(COLLAPSE_FLOOR);

        // Sustainability: at a rate that was truly verified the producer keeps up, so the average
        // send delay over the measurement window stays small. A rate that only survived discovery by
        // bursting shows delay climbing into the seconds -- the signature of an over-confirm.
        assertThat(discovery.publishDelayAvgMs)
                .as(
                        "confirmed rate %s msg/s should be sustainable, but the measurement window's"
                                + " average publish delay says the producer could not keep up",
                        discovery.confirmedRate)
                .isLessThan(500.0);

        // The reported rate must be one the measurement window actually delivered. This is what the
        // 1.8M-msg/s incident violated: CHOP certified a rate its own measurement window then missed
        // by 25%. rampVerification.rate is now the achieved throughput of the confirmation hold, so
        // the window running at that rate should match it closely.
        assertThat(discovery.measuredPublishRate)
                .as(
                        "measurement window achieved %s msg/s while discovery reported %s msg/s",
                        discovery.measuredPublishRate, discovery.confirmedRate)
                .isGreaterThan(0.85 * discovery.confirmedRate);
    }

    private static final double COLLAPSE_FLOOR = 500.0;

    /** What one discovery run produced, plus how the measurement window that followed behaved. */
    private static final class Discovery {
        final Double confirmedRate;
        final double publishDelayAvgMs;
        final double measuredPublishRate;

        Discovery(Double confirmedRate, double publishDelayAvgMs, double measuredPublishRate) {
            this.confirmedRate = confirmedRate;
            this.publishDelayAvgMs = publishDelayAvgMs;
            this.measuredPublishRate = measuredPublishRate;
        }
    }

    private static Discovery runDiscovery(RampVerdict verdict) throws Exception {
        File driverConfig = writeKafkaDriverConfig(KAFKA.getBootstrapServers());

        Workload workload = discoveryWorkload(verdict);
        LocalWorker worker = new LocalWorker();
        worker.initializeDriver(driverConfig);

        TestResult result;
        try (WorkloadGenerator generator = new WorkloadGenerator("Kafka", workload, worker)) {
            result = generator.run();
        }

        Double confirmedRate = result.rampVerification == null ? null : result.rampVerification.rate;
        double publishDelayAvgMs = result.aggregatedPublishDelayLatencyAvg / 1000.0;
        // Median, not mean. LocalWorker.resetStats() only calls stats.resetLatencies() and never
        // stats.reset(), so the message counters are NOT cleared between discovery and measurement:
        // the first interval of a producerRate:0 run reports every message published during
        // discovery as if it arrived in that one 10s window. Observed at 15,439,670 msg/s against a
        // steady-state 481,000. A mean is wrecked by that single sample; the median ignores it.
        double measuredPublishRate =
                result.publishRate.stream()
                        .mapToDouble(Double::doubleValue)
                        .sorted()
                        .skip(Math.max(0, result.publishRate.size() / 2))
                        .findFirst()
                        .orElse(0.0);
        long maxBacklog = result.backlog.stream().mapToLong(Long::longValue).max().orElse(0L);
        log.info(
                "{} confirmedRate={} msg/s | measurement window: achieved={} msg/s,"
                        + " avgPublishDelay={} ms, maxBacklog={} msgs",
                verdict,
                confirmedRate,
                String.format("%.0f", measuredPublishRate),
                String.format("%.1f", publishDelayAvgMs),
                maxBacklog);
        return new Discovery(confirmedRate, publishDelayAvgMs, measuredPublishRate);
    }

    private static Workload discoveryWorkload(RampVerdict verdict) {
        Workload workload = new Workload();
        workload.name = "chop-sustainability-it-" + verdict;
        workload.topics = 1;
        workload.partitionsPerTopic = 10;
        workload.messageSize = 100;
        // Build the payload in-memory so no payload/*.data file is needed.
        workload.useRandomizedPayloads = true;
        workload.randomBytesRatio = 1.0;
        workload.randomizedPayloadPoolSize = 1;
        workload.subscriptionsPerTopic = 1;
        workload.producersPerTopic = 1;
        workload.consumerPerSubscription = 1;

        workload.producerRate = 0; // discover the rate
        workload.rampAlgorithm = RampAlgorithm.CHOP;
        workload.rampVerdict = verdict;
        // Start LOW, and resist the temptation to start near the expected knee to save holds.
        // Bracket's two directions are not symmetric under hysteresis. Climbing from below keeps
        // every early candidate in the healthy regime, so lo is banked from genuinely clean
        // readings and the knee is crossed exactly once. Starting above the knee inverts that: the
        // first hold overloads the broker, and because rampSettleSeconds runs only once before
        // bracket, nothing drains that fallout before the halved candidate is judged -- so it fails
        // for reasons unrelated to its own rate, and the halving cascades without ever establishing
        // a lo. Doubling is logarithmic, so starting low costs only a few extra holds.
        workload.rampStartRate = 5000;
        workload.rampMaxBacklogSeconds = 0.1;

        // Hold length is the lever that decides whether a bursting broker can masquerade an
        // unsustainable rate as clean, so it is set generously and, crucially, bracket is held to
        // the SAME duration as chop. A short bracket hold over-confirms a high rate into lo, and lo
        // only ever moves upward -- so no chop hold length can recover from it afterwards. This is
        // why rampBracketHoldSeconds defaults to rampHoldSeconds; do not shorten it here to save
        // wall-clock.
        workload.rampBracketPeriodSeconds = 2;
        workload.rampSettleSeconds = 10;
        workload.rampBracketHoldSeconds = 45;
        workload.rampHoldSeconds = 45;
        workload.rampConvergenceTolerance = 0.05;
        workload.rampMaxDiscoveryMinutes = 15; // safety cap, sized for 45s holds

        workload.consumerBacklogSizeGB = 0;
        workload.warmupDurationMinutes = 0;
        // Deliberately longer than the hold (45s): if the window were the same length, "held for a
        // hold" and "sustained for the window" would be the same claim and the check would not be
        // independent of the thing it is checking.
        workload.testDurationMinutes = 2;
        return workload;
    }

    private static File writeKafkaDriverConfig(String bootstrapServers) throws Exception {
        // Single-broker container: replicationFactor 1 and min.insync.replicas 1 (the shipped
        // example uses 3/2, which cannot be satisfied here). cp-kafka's Testcontainers image
        // already forces the __consumer_offsets replication factor to 1 for single-node.
        String yaml =
                "name: Kafka\n"
                        + "driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver\n"
                        + "replicationFactor: 1\n"
                        + "topicConfig: |\n"
                        + "  min.insync.replicas=1\n"
                        + "commonConfig: |\n"
                        + "  bootstrap.servers="
                        + bootstrapServers
                        + "\n"
                        + "producerConfig: |\n"
                        + "  acks=all\n"
                        + "  linger.ms=1\n"
                        + "consumerConfig: |\n"
                        + "  auto.offset.reset=earliest\n"
                        + "  enable.auto.commit=false\n";
        File file = Files.createTempFile("kafka-driver-it", ".yaml").toFile();
        file.deleteOnExit();
        Files.write(file.toPath(), yaml.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
