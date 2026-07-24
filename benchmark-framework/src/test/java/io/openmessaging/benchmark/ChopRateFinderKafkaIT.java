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
     * This drives real discovery against a real broker with a realistic starting rate, then checks
     * the measurement window that ran at the confirmed rate -- if that rate is genuinely sustainable,
     * publish-delay (the rate limiter's backlog of un-sent messages) stays bounded rather than
     * growing without limit. This is the end-to-end analogue of the manual AKS check that first
     * exposed CHOP over-confirming (converged 8062 msg/s, then 35k messages of backlog in the very
     * next window).
     */
    @Test
    void chopConfirmedRateIsActuallySustainableAgainstARealBroker() throws Exception {
        File driverConfig = writeKafkaDriverConfig(KAFKA.getBootstrapServers());

        Workload workload = discoveryWorkload();
        LocalWorker worker = new LocalWorker();
        worker.initializeDriver(driverConfig);

        TestResult result;
        try (WorkloadGenerator generator = new WorkloadGenerator("Kafka", workload, worker)) {
            result = generator.run();
        }

        Double confirmedRate = result.rampVerification == null ? null : result.rampVerification.rate;
        double pubDelayAvgMs = result.aggregatedPublishDelayLatencyAvg / 1000.0;
        long maxBacklog = result.backlog.stream().mapToLong(Long::longValue).max().orElse(0L);
        log.info(
                "CHOP confirmedRate={} msg/s | measurement window: avgPublishDelay={} ms,"
                        + " maxBacklog={} msgs",
                confirmedRate,
                String.format("%.1f", pubDelayAvgMs),
                maxBacklog);

        // Sustainability: at a rate that was truly verified, the producer keeps up, so the average
        // send delay over the measurement window stays small. A rate that only survived discovery
        // by bursting shows delay climbing into the seconds -- the signature of an over-confirm.
        assertThat(pubDelayAvgMs)
                .as(
                        "confirmed rate %s msg/s should be sustainable, but the measurement window's"
                                + " average publish delay indicates the producer could not keep up",
                        confirmedRate)
                .isLessThan(500.0);
    }

    private static Workload discoveryWorkload() {
        Workload workload = new Workload();
        workload.name = "chop-sustainability-it";
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
        workload.rampStartRate = 5000; // a realistic starting point, not an adversarial one
        workload.rampMaxBacklogSeconds = 0.1;

        // Short timings so the IT finishes quickly; the clamp behaviour is per-poll, not
        // hold-length dependent, so shrinking these doesn't weaken what's under test.
        workload.rampBracketPeriodSeconds = 2;
        workload.rampSettleSeconds = 5;
        workload.rampBracketHoldSeconds = 15;
        workload.rampHoldSeconds = 20; // long enough that a burst cannot masquerade as sustainable
        workload.rampConvergenceTolerance = 0.05;
        workload.rampMaxDiscoveryMinutes = 6; // safety cap

        workload.consumerBacklogSizeGB = 0;
        workload.warmupDurationMinutes = 0;
        workload.testDurationMinutes = 1; // measurement window whose delay reveals (un)sustainability
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
