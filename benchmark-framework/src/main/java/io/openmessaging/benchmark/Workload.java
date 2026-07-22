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


import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.util.Map;

public class Workload {
    public String name;

    /** Number of topics to create in the test. */
    public int topics;

    /** Number of partitions each topic will contain. */
    public int partitionsPerTopic;

    public KeyDistributorType keyDistributor = KeyDistributorType.NO_KEY;

    public int messageSize;

    /**
     * Message size distribution for variable-sized payloads. Keys are size ranges (e.g., "0-256",
     * "256-1024", "1KB-4KB"), values are relative weights. Mutually exclusive with messageSize - if
     * set, messageSize is ignored.
     */
    public Map<String, Integer> messageSizeDistribution;

    /**
     * Returns true if this workload uses a size distribution instead of fixed size.
     *
     * @return true if messageSizeDistribution is configured, false otherwise
     */
    public boolean usesDistribution() {
        return messageSizeDistribution != null && !messageSizeDistribution.isEmpty();
    }

    public boolean useRandomizedPayloads;
    public double randomBytesRatio;
    public int randomizedPayloadPoolSize;

    public String payloadFile;

    public int subscriptionsPerTopic;

    public int producersPerTopic;

    public int consumerPerSubscription;

    public int producerRate;

    /**
     * The following fields only apply when producerRate == 0, i.e. when the generator probes for
     * the maximum sustainable rate (a "ramp test"). Leave unset to fall back to the existing
     * env-var / hardcoded defaults.
     */
    public RampAlgorithm rampAlgorithm = RampAlgorithm.AIMD;

    /** Initial probe rate for ramp discovery. Defaults to 10000 if unset. */
    public Integer rampStartRate;

    /** Publish backlog limit used by ramp discovery. Defaults to env PUBLISH_BACKLOG_LIMIT, else 1000. */
    public Long rampPublishBacklogLimit;

    /** Receive backlog limit used by ramp discovery. Defaults to env RECEIVE_BACKLOG_LIMIT, else 1000. */
    public Long rampReceiveBacklogLimit;

    /** CHOP only: seconds between bracket-phase steps / hold-phase polls. Defaults to 3. */
    public Integer rampBracketPeriodSeconds;

    /** CHOP only: seconds to hold and verify each candidate rate. Defaults to 30. */
    public Integer rampHoldSeconds;

    /** CHOP only: relative (hi - lo) / lo band at which to stop chopping. Defaults to 0.05. */
    public Double rampConvergenceTolerance;

    /** CHOP only: safety cap on total discovery time, in minutes. Defaults to 10. */
    public Integer rampMaxDiscoveryMinutes;

    /**
     * If the consumer backlog is > 0, the generator will accumulate messages until the requested
     * amount of storage is retained and then it will start the consumers to drain it.
     *
     * <p>The testDurationMinutes will be overruled to allow the test to complete when the consumer
     * has drained all the backlog and it's on par with the producer
     */
    public long consumerBacklogSizeGB = 0;
    /**
     * The ratio of the backlog that can remain and yet the backlog still be considered empty, and
     * thus the workload can complete at the end of the configured duration. In some systems it is not
     * feasible for the backlog to be drained fully and thus the workload will run indefinitely. In
     * such circumstances, one may be content to achieve a partial drain such as 99% of the backlog.
     * The value should be on somewhere between 0.0 and 1.0, where 1.0 indicates that the backlog
     * should be fully drained, and 0.0 indicates a best effort, where the workload will complete
     * after the specified time irrespective of how much of the backlog has been drained.
     */
    public double backlogDrainRatio = 1.0;

    public int testDurationMinutes;

    public int warmupDurationMinutes = 1;
}
