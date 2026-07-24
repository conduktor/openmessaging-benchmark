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
     * The following fields only apply when producerRate == 0, i.e. when the generator probes for the
     * maximum sustainable rate (a "ramp test"). Leave unset to fall back to the existing env-var /
     * hardcoded defaults.
     */
    public RampAlgorithm rampAlgorithm = RampAlgorithm.AIMD;

    public RampVerdict rampVerdict = RampVerdict.BACKLOG;

    // Minimum fraction of target throughput (and of consumer drain) a candidate must achieve over a
    // hold to count as clean under rampVerdict: THROUGHPUT. Null -> default 0.95 in RampRateFinder.
    public Double rampMinThroughputRatio;

    /** Initial probe rate for ramp discovery. Defaults to 10000 if unset. */
    public Integer rampStartRate;

    /**
     * Publish backlog limit used by ramp discovery. Defaults to env PUBLISH_BACKLOG_LIMIT, else 1000.
     */
    public Long rampPublishBacklogLimit;

    /**
     * Receive backlog limit used by ramp discovery. Defaults to env RECEIVE_BACKLOG_LIMIT, else 1000.
     */
    public Long rampReceiveBacklogLimit;

    /**
     * CHOP only: backlog limit expressed as seconds' worth of the candidate rate (limit = currentRate
     * * rampMaxBacklogSeconds) instead of a fixed message count, so the check is equally strict at
     * every rate tested during the bracket phase's exponential range. When set, this replaces
     * rampPublishBacklogLimit/rampReceiveBacklogLimit for CHOP. Unset by default.
     */
    public Double rampMaxBacklogSeconds;

    /**
     * CHOP only: hard floor (in messages) on the limit rampMaxBacklogSeconds computes. Without it, a
     * single early false failure halves the candidate rate and, in the same stroke, halves the
     * tolerance too -- the wrong direction for a recovery mechanism -- letting one bad reading
     * cascade all the way down to a near-zero "confirmed" rate. Defaults to 1000, matching the old
     * fixed-count default so the relative check can never become stricter than a fixed-count check
     * would have been. Only meaningful when rampMaxBacklogSeconds is set.
     */
    public Long rampMaxBacklogFloor;

    /**
     * CHOP only: hard cap (in messages) on the limit rampMaxBacklogSeconds computes, so the tolerance
     * can't grow unbounded as bracket's exponential doubling searches far past the real ceiling --
     * without it, a high enough candidate rate can make the relative check tolerate an enormous
     * backlog and report a wildly implausible "confirmed" rate. Only meaningful when
     * rampMaxBacklogSeconds is set. Defaults to 100000.
     */
    public Long rampMaxBacklogCeiling;

    /** CHOP only: seconds between bracket-phase steps / hold-phase polls. Defaults to 3. */
    public Integer rampBracketPeriodSeconds;

    /**
     * CHOP only: seconds to run at rampStartRate before backlog is evaluated at all, so a
     * consumer-group rebalance tail or producer connection warm-up still settling right after the
     * load starts can never be mistaken for the candidate rate being unsustainable. Defaults to 30.
     */
    public Integer rampSettleSeconds;

    /**
     * CHOP only: seconds a bracket-phase candidate (the exponential doubling/halving search that
     * finds the initial [lo, hi] window) must hold clean before being accepted. Defaults to the
     * resolved rampHoldSeconds, i.e. bracket and chop are equally rigorous unless you explicitly
     * shorten this once you trust bracket's coarser candidates need less scrutiny.
     */
    public Integer rampBracketHoldSeconds;

    /** CHOP only: seconds to hold and verify each chop-phase candidate. Defaults to 30. */
    public Integer rampHoldSeconds;

    /**
     * CHOP only: number of consecutive clean confirmation holds required at the same rate before
     * accepting it, beyond the initial tolerance-meeting hold. Defaults to 1. Raising this trades
     * discovery time for confidence that the accepted rate isn't a one-off pass.
     */
    public Integer rampConfirmationHolds;

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
