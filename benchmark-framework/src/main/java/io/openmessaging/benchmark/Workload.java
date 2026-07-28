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
     * rampMaxBacklogSeconds is set. Defaults to 500000.
     *
     * <p>Watch the interaction with rampMaxBacklogSeconds: the limit is {@code max(floor, min(rate x
     * seconds, ceiling))}, so once {@code rate x seconds} exceeds the ceiling the ceiling wins and
     * the limit stops scaling with rate. At the 100000 this used to default to, that happened above
     * roughly 200000 msg/s -- an AKS run asking for 0.5s at 589000 msg/s silently got 0.17s worth,
     * which is what tripped its one false failure. If you run above 1000000 msg/s, raise this too or
     * the rate-scaled limit quietly becomes a fixed count again.
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

    /**
     * CHOP only: upper bound, in seconds, on the recovery period run after a candidate rate fails. A
     * failed candidate leaves the system carrying its overshoot (deep queues, consumer lag, GC
     * pressure); judging the next candidate immediately measures that fallout instead of the
     * candidate, and since the bracket phase only ever raises lo, one contaminated reading cannot be
     * recovered from. During recovery the load runs at the highest rate already known to be
     * sustainable and nothing is evaluated. This is a cap, not a fixed wait: recovery ends as soon as
     * the backlog is back within the limit it is judged against, so when there is nothing to drain it
     * costs a single poll.
     *
     * <p>Defaults to 0 (off), because enabling it changes the search trajectory after every failed
     * candidate; a good starting value is the resolved rampHoldSeconds.
     */
    public Integer rampDrainSeconds;

    /**
     * CHOP only: seconds to hold and verify each chop-phase candidate. Defaults to 180.
     *
     * <p>This is the single most consequential ramp setting, because it is what decides whether a
     * broker can absorb an oversubscribed rate for the whole hold and so look clean. A broker acks at
     * the full target rate until its page cache, batching and socket buffers saturate; only then does
     * throughput droop. A hold shorter than that absorption time accepts a rate that the measurement
     * window afterwards fails on, and no verdict predicate can detect it from inside the hold.
     *
     * <p>180 rather than the earlier 30 because 30 is not survivable on real hardware: absorption
     * scales with the cache the cluster has. Size it from the cluster, not from this default --
     * roughly buffer-depth / overshoot -- and note higher candidate rates need longer holds, not
     * equal ones. Raising this raises discovery time proportionally, so check rampMaxDiscoveryMinutes
     * with it.
     */
    public Integer rampHoldSeconds;

    /**
     * CHOP only: number of consecutive clean confirmation holds required at the same rate before
     * accepting it, beyond the initial tolerance-meeting hold. Defaults to 1. Raising this trades
     * discovery time for confidence that the accepted rate isn't a one-off pass.
     */
    public Integer rampConfirmationHolds;

    /**
     * CHOP only: how many *consecutive* polls must breach the backlog limit before a candidate is
     * failed. Defaults to 2. Only applies to rampVerdict: BACKLOG, which is the mode that decides
     * per-poll; THROUGHPUT is decided once at hold completion and is unaffected.
     *
     * <p>1 restores the old behaviour of condemning a candidate on a single sample. That is a coin
     * toss near the boundary, and because nothing ever reopens hi, the mistake is permanent: on AKS a
     * 640,000 msg/s candidate ran seven consecutive healthy polls, dipped for one poll 10% past the
     * limit, and capped the whole search 7% low -- on a hold whose own aggregate was 99.13% of
     * target. Genuine overload does not look like that; the same trial measured backlog jumping ~57x
     * in one poll and then staying elevated for 12-15s. So the second poll costs almost nothing
     * against a real failure and rejects a transient outright.
     */
    public Integer rampBreachPolls;

    /** CHOP only: relative (hi - lo) / lo band at which to stop chopping. Defaults to 0.05. */
    public Double rampConvergenceTolerance;

    /**
     * CHOP only: when a candidate rate fails, seed the next candidate from the throughput that
     * candidate actually achieved rather than bisecting the bracket blindly. A failed hold has
     * already measured what the system can do -- published / elapsed is a direct capacity estimate --
     * so bisection throws away a measurement the run just paid for.
     *
     * <p>The estimate is only used when it is informative and safe: it must sit at least
     * rampConvergenceTolerance below the rate that just failed (a candidate that failed on consumer
     * lag while publishing at its full target says nothing about producer capacity, and a seed within
     * the tolerance band is inside the noise the search already ignores), and strictly above the
     * highest rate already known to hold. Otherwise the bracket is bisected as before.
     *
     * <p>Defaults to false, because it changes the search trajectory after every failed candidate.
     */
    public Boolean rampSeedFromAchievedRate;

    /**
     * CHOP only: safety cap on total discovery time, in minutes. Defaults to 60.
     *
     * <p>Coupled to rampHoldSeconds. Under rampVerdict: THROUGHPUT there is no per-poll fast-fail, so
     * every candidate costs a full hold -- including the bracket phase's doomed 2x overshoots -- and
     * a search from the default start rate to a 7-figure ceiling runs roughly 15 holds. Budget about
     * {@code settle + 15 x rampHoldSeconds}, plus the drain if rampDrainSeconds is set. Hitting the
     * cap is not a failure: discovery reports the best rate that held, having never confirmed it,
     * with a WARN and no rampVerification attached.
     */
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
