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
import static org.assertj.core.api.Assertions.within;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RampRateFinderTest {

    private static Workload workload() {
        Workload workload = new Workload();
        workload.subscriptionsPerTopic = 1; // one delivery per publish, so received tracks published
        workload.rampPublishBacklogLimit = 100L;
        workload.rampReceiveBacklogLimit = 100L;
        workload.rampSettleSeconds = 0; // most tests don't care about settling; a few override it
        // Pinned off so a failure's trajectory is the search's own. rampDrainSeconds now defaults ON
        // (to the resolved rampHoldSeconds), which inserts a recovery period after every failed
        // candidate -- correct for real runs, but it makes every hand-fed poll sequence below about the
        // drain as well as its own subject. The drain has dedicated tests, and
        // theDefaultDiscoveryBudgetCoversASearchAtTheDefaultHold drives a whole search on the shipped
        // defaults, so the on-path is covered where it matters.
        workload.rampDrainSeconds = 0;
        return workload;
    }

    @Test
    void settlePeriodIgnoresBacklogEntirelyBeforeRealEvaluationBegins() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampSettleSeconds = 6;
        workload.rampBracketHoldSeconds = 3;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        // A huge, transient apparent backlog during settling -- e.g. residual consumer-group
        // rebalance catch-up right after startLoad() -- must not be evaluated at all.
        boolean done = finder.poll(periodNanos, 100, 0);
        assertThat(done).isFalse();
        assertThat(finder.isSettled()).isFalse();
        assertThat(finder.getLo()).isNull();
        assertThat(finder.getHi()).isNull();

        // 6s total elapsed -- settle threshold reached on this call, but this call only marks
        // settled and resets the baseline; it doesn't evaluate backlog either.
        done = finder.poll(periodNanos, 200, 100);
        assertThat(done).isFalse();
        assertThat(finder.isSettled()).isTrue();
        assertThat(finder.getLo()).isNull();
        assertThat(finder.getHi()).isNull();

        // First real evaluation, using the baseline reset at the end of settling: clean.
        done = finder.poll(periodNanos, 200 + 3000, 200 + 3000);
        assertThat(done).isFalse();
        assertThat(finder.getLo()).isEqualTo(1000.0);
    }

    @Test
    void bracketDoublesUntilBacklogThenTransitionsToChop() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 3; // one poll completes a bracket hold too (default ties them)
        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(4500);
        long periodNanos = SECONDS.toNanos(3);

        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.BRACKET);

        finder.poll(periodNanos, 0, 0); // settle
        assertThat(finder.isSettled()).isTrue();

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
        assertThat(finder.isConfirming()).isTrue();
        assertThat(finder.isConfirmed()).isTrue();
        // The backlog limit tolerates a small overshoot past true capacity before it's detected,
        // so the discovered rate lands just above 4500, not below it -- assert closeness instead.
        assertThat(Math.abs(4500.0 - finder.getCurrentRate()) / 4500.0).isLessThan(0.05);
    }

    @Test
    void holdFailsPartwayExitsEarlyWithoutWaitingOutTheFullHold() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 3; // fast bracket setup
        workload.rampHoldSeconds = 30; // long chop hold -- a single failing poll must not wait this out
        workload.rampConvergenceTolerance = 0.05;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle

        // Bracket: 1000 clean, 2000 exceeds -> CHOP with lo=1000, hi=2000
        finder.poll(periodNanos, 3000, 3000);
        finder.poll(periodNanos, 3000 + 3000, 3000 + 3000);
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);

        // Bracket held for 3s where chop holds for 30, so chop re-verifies lo at full length first.
        // Ten clean 3s polls at 1000 msg/s, then the bracket is bisected as usual.
        assertThat(finder.getCurrentRate()).isEqualTo(1000.0);
        long total = 6000;
        for (int i = 0; i < 10; i++) {
            total += 3000;
            finder.poll(periodNanos, total, total);
        }
        assertThat(finder.getCurrentRate()).isEqualTo(1500.0);

        // First hold poll at 1500 is clean (published keeps up with expected)
        finder.poll(periodNanos, total + 4500, total + 4500);
        assertThat(finder.getHi()).isEqualTo(2000.0); // unchanged so far

        // Two consecutive breaching polls at 1500 -- must fail on the second, not after the full 30s.
        // rampBreachPolls defaults to 2, so a single sample no longer condemns a candidate.
        finder.poll(periodNanos, total + 4500 + 1000, total + 4500 + 1000);
        boolean done = finder.poll(periodNanos, total + 4500 + 2000, total + 4500 + 2000);

        assertThat(done).isFalse();
        assertThat(finder.getHi()).isEqualTo(1500.0);
        assertThat(finder.getCurrentRate()).isEqualTo((1000.0 + 1500.0) / 2.0);
        // Tolerance was never met, so the confirmation hold was never entered.
        assertThat(finder.isConfirming()).isFalse();
        assertThat(finder.isConfirmed()).isFalse();
    }

    @Test
    void failedConfirmationReopensTheSearchInsteadOfAcceptingAnUnverifiedRate() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 3; // one poll completes a hold (bracket and chop alike)
        workload.rampConvergenceTolerance = 0.5; // converge quickly for this test
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle

        // Bracket: 1000 clean, 2000 exceeds -> CHOP, lo=1000, hi=2000, mid=1500
        finder.poll(periodNanos, 3000, 3000);
        finder.poll(periodNanos, 6000, 6000);
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
        assertThat(finder.isConfirming()).isFalse();

        // Hold at 1500 passes cleanly -- within tolerance (500/1500 = 0.33 <= 0.5), so this
        // triggers the one-more confirmation hold rather than DONE yet, at lo x (1 - 0.5) = 750
        // rather than at 1500 itself. This is the poll() call where isConfirming() flips
        // false -> true.
        boolean done = finder.poll(periodNanos, 6000 + 4500, 6000 + 4500);
        assertThat(done).isFalse();
        assertThat(finder.getCurrentRate()).isEqualTo(750.0);
        assertThat(finder.isConfirming()).isTrue();
        assertThat(finder.isConfirmed()).isFalse();

        // The confirmation hold runs at lo x (1 - tolerance) = 750, and now shows a backlog breach.
        // A rate *below* one that already passed failing is a genuine contradiction, so
        // nonMonotonic latches -- unlike the old same-rate re-check, where a flip at the knee was
        // just boundary noise being recorded as instability. The search must reopen rather than
        // accept: tighten hi to the rate that failed.
        //
        // Note the interaction this exposes: because the confirmation rate sits below every rate
        // recorded as passing, bestKnownPassBelow(750) finds nothing and falls back to its blind
        // 0.9 backoff (675) for the new lo. With the 0.05 default the confirm rate is only 5% under
        // lo, so a real recorded pass is normally available; this test's outsized 0.5 tolerance is
        // what pushes it past all of them.
        done = finder.poll(periodNanos, 10500 + 2000, 10500 + 2000);

        assertThat(done).isFalse();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
        assertThat(finder.isNonMonotonic()).isTrue();
        assertThat(finder.isConfirming()).isFalse();
        assertThat(finder.isConfirmed()).isFalse();
        assertThat(finder.getLo()).isEqualTo(675.0);
        assertThat(finder.getHi()).isEqualTo(750.0);
        assertThat(finder.getCurrentRate()).isEqualTo(712.5);

        // The reopened search can still converge and genuinely confirm on a new, lower candidate.
        done = finder.poll(periodNanos, 12500 + 3750, 12500 + 3750); // clean hold @712.5
        assertThat(done).isFalse();
        assertThat(finder.isConfirming()).isTrue();

        done = finder.poll(periodNanos, 16250 + 3750, 16250 + 3750); // clean confirmation hold
        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.isConfirmed()).isTrue();
        assertThat(finder.getCurrentRate()).isEqualTo(356.25); // 712.5 x (1 - 0.5)
        // The earlier contradiction is still surfaced even though discovery ultimately confirmed
        // a (different, lower) rate -- it's evidence the system showed unstable behavior at all.
        assertThat(finder.isNonMonotonic()).isTrue();
    }

    @Test
    void multipleConfirmationHoldsAreRequiredWhenConfigured() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 3;
        workload.rampConvergenceTolerance = 0.5;
        workload.rampConfirmationHolds = 2;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle
        finder.poll(periodNanos, 3000, 3000); // bracket: 1000 clean
        finder.poll(periodNanos, 6000, 6000); // bracket: 2000 exceeds -> CHOP mid=1500

        boolean done = finder.poll(periodNanos, 10500, 10500); // first hold @1500 passes
        assertThat(done).isFalse();
        assertThat(finder.isConfirming()).isTrue();

        done = finder.poll(periodNanos, 15000, 15000); // 1st confirmation hold passes
        assertThat(done).isFalse(); // still short of the 2 required confirmation holds
        assertThat(finder.isConfirmed()).isFalse();

        done = finder.poll(periodNanos, 19500, 19500); // 2nd confirmation hold passes
        assertThat(done).isTrue();
        assertThat(finder.isConfirmed()).isTrue();
        // Confirmation holds run at lo x (1 - convergenceTolerance), i.e. deliberately just below
        // the knee. This test uses an outsized 0.5 tolerance purely so chop converges in one step,
        // so the safety margin it produces is correspondingly outsized: 1500 x 0.5. With the 0.05
        // default the margin is 5%.
        assertThat(finder.getCurrentRate()).isEqualTo(750.0);
    }

    @Test
    void relativeBacklogLimitScalesWithCandidateRateUnlikeTheFixedAbsoluteLimit() {
        long periodNanos = SECONDS.toNanos(1);

        // A 500-message *receive* backlog at 100,000 msg/s: negligible in relative terms (5ms worth)
        // but far above the small fixed absolute default (100 messages) that workload() sets. The
        // fixture drives receive backlog rather than publish shortfall because the producer side is now
        // judged as a fraction of target, which no rate-scaled or fixed message count applies to.
        Workload absolute = workload();
        absolute.rampStartRate = 100000;
        RampRateFinder absoluteFinder = new RampRateFinder(absolute);
        absoluteFinder.poll(periodNanos, 0, 0); // settle
        // Two consecutive breaching polls: rampBreachPolls defaults to 2, so one sample no longer
        // condemns a candidate (see aSingleBreachingPollDoesNotFailACandidateThatIsOtherwiseKeepingUp).
        absoluteFinder.poll(periodNanos, 100_000, 99_500);
        absoluteFinder.poll(periodNanos, 200_000, 199_500);
        assertThat(absoluteFinder.getHi()).isEqualTo(100000.0); // absolute limit (100) blown
        assertThat(absoluteFinder.getLo()).isNull();

        Workload relative = workload();
        relative.rampStartRate = 100000;
        relative.rampMaxBacklogSeconds = 0.01; // 1,000 messages allowed at this rate
        relative.rampBracketHoldSeconds = 1; // one poll completes the hold for a clean verdict
        RampRateFinder relativeFinder = new RampRateFinder(relative);
        relativeFinder.poll(periodNanos, 0, 0); // settle
        relativeFinder.poll(periodNanos, 100_000, 99_500);
        // Same backlog, but well under the rate-scaled limit -> treated as clean instead.
        assertThat(relativeFinder.getLo()).isEqualTo(100000.0);
        assertThat(relativeFinder.getHi()).isNull();
    }

    @Test
    void aRunawayToleranceCannotHideAGrossShortfallNowThePublishSideIsARatio() {
        // This used to document a pathology: at 1,000,000 msg/s with rampMaxBacklogSeconds 1.0, the
        // relative limit alone tolerated a full 1,000,000-message shortfall, so a candidate that
        // published 5,000 of an expected 1,000,000 was called "clean" and the search reported a
        // two-orders-of-magnitude-wrong rate as confirmed. The ceiling was added to bound that.
        //
        // Judging the producer side as a fraction rather than a count removes the pathology at its
        // source: 5,000 of 1,000,000 is 0.005 against a 0.95 gate, and no tolerance expressed in
        // messages enters into it. The ceiling still matters for the *consumer* side, which is a level.
        Workload workload = workload();
        workload.rampStartRate = 1000000;
        workload.rampMaxBacklogSeconds = 1.0;
        workload.rampMaxBacklogCeiling = 2_000_000L; // deliberately too high to catch anything
        workload.rampBracketHoldSeconds = 1;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        finder.poll(periodNanos, 5000, 5000);

        assertThat(finder.getHi())
                .as("0.005 of target is caught by the ratio, with no help from the ceiling")
                .isEqualTo(1000000.0);
        assertThat(finder.getLo()).isNull();
    }

    @Test
    void backlogCeilingCapsTheRelativeLimitAtHighCandidateRates() {
        Workload workload = workload();
        workload.rampStartRate = 1000000;
        workload.rampMaxBacklogSeconds = 1.0; // would allow 1,000,000 messages without a ceiling
        workload.rampMaxBacklogCeiling = 1000L; // deliberately low so the cap is what decides
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        // Two consecutive breaching polls: rampBreachPolls defaults to 2, so one sample no longer
        // condemns a candidate (see aSingleBreachingPollDoesNotFailACandidateThatIsOtherwiseKeepingUp).
        finder.poll(periodNanos, 5000, 5000); // same massive shortfall as above
        finder.poll(periodNanos, 10000, 10000);

        assertThat(finder.getHi()).isEqualTo(1000000.0); // caught by the ceiling this time
        assertThat(finder.getLo()).isNull();
    }

    @Test
    void theDefaultCeilingDoesNotOverrideAModestBacklogSecondsAtRealClusterRates() {
        // The AKS trial set rampMaxBacklogSeconds: 0.5 and never got it. At its 589,000 msg/s answer
        // 0.5s is 294,465 messages, but the old 100,000 default ceiling clamped that to 0.17s worth --
        // so above ~200,000 msg/s the rate-scaled limit silently reverted to a fixed count, the very
        // thing rate-scaling exists to replace. It also caused that run's one false failure: a single
        // poll's 110,055-message publish shortfall tripped a limit that should have been 320,000.
        //
        // Here: 640,000 msg/s carrying a steady 150,000 messages of receive backlog. That is 0.23s
        // worth, well inside the configured 0.5s, and must be clean.
        Workload workload = workload();
        workload.rampStartRate = 640000;
        workload.rampMaxBacklogSeconds = 0.5; // 320,000 messages at this rate
        workload.rampBracketHoldSeconds = 2; // two 1s polls complete the hold
        RampRateFinder finder = new RampRateFinder(workload); // ceiling left unset -> default
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 640_000, 490_000); // at target, 150,000 behind on receive
        finder.poll(periodNanos, 1_280_000, 1_130_000); // still 150,000 behind, steady

        assertThat(finder.getLo())
                .as("150,000 is 0.23s at this rate, inside the configured 0.5s")
                .isEqualTo(640000.0);
        assertThat(finder.getHi()).isNull();
    }

    @Test
    void theDefaultCeilingStillCatchesARunawayToleranceAtVeryHighRates() {
        // The ceiling's original purpose, which raising it must not give up. The incident it was added
        // for: a 1,000,000+ msg/s candidate with rampMaxBacklogSeconds 1.0, where an uncapped limit let
        // a million messages of backlog count as clean and the search reported a
        // two-orders-of-magnitude-wrong rate as confirmed. 700,000 messages is inside that uncapped
        // 1.0s limit but past the ceiling, so the ceiling is what has to reject it. (This one passes
        // before and after the raise -- it is the guard on the raise, not a demonstration of it.)
        Workload workload = workload();
        workload.rampStartRate = 1000000;
        workload.rampMaxBacklogSeconds = 1.0; // 1,000,000 messages uncapped
        RampRateFinder finder = new RampRateFinder(workload); // ceiling left unset -> default
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1_000_000, 300_000); // 700,000 behind
        finder.poll(periodNanos, 2_000_000, 1_300_000); // still 700,000 behind -- two consecutive

        assertThat(finder.getHi())
                .as("700,000 must still be rejected, or the runaway incident returns")
                .isEqualTo(1000000.0);
        assertThat(finder.getLo()).isNull();
    }

    @Test
    void uncappedRelativeLimitCanBecomeStricterThanUsefulAtLowRates() {
        // The symmetric incident to the ceiling one above: at a low candidate rate,
        // rampMaxBacklogSeconds alone gives a tiny tolerance -- smaller than a fixed-count check
        // would ever have been -- so an ordinary, harmless blip gets treated as a capacity
        // failure. Without a floor high enough to catch it, one such false failure (which halves
        // the rate, and in the same stroke halves the tolerance again) can cascade all the way
        // down to a near-zero "confirmed" rate.
        Workload workload = workload();
        workload.rampStartRate = 100;
        workload.rampMaxBacklogSeconds = 0.1; // limit would be only 10 messages at this rate
        workload.rampMaxBacklogFloor = 1L; // deliberately low so the floor isn't what saves this
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        // Publishing on target, but 50 messages behind on the receive side. Two consecutive breaching
        // polls, since rampBreachPolls defaults to 2.
        finder.poll(periodNanos, 100, 50);
        finder.poll(periodNanos, 200, 150);

        assertThat(finder.getHi()).isEqualTo(100.0); // wrongly treated as a capacity failure
        assertThat(finder.getLo()).isNull();
    }

    @Test
    void backlogFloorPreventsToleranceFromShrinkingBelowFloorAtLowRates() {
        Workload workload = workload();
        workload.rampStartRate = 100;
        workload.rampMaxBacklogSeconds = 0.1; // limit would be only 10 messages without a floor
        workload.rampMaxBacklogFloor = 1000L;
        workload.rampBracketHoldSeconds = 1; // one poll completes the hold for a clean verdict
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        finder.poll(periodNanos, 100, 50); // same 50-message receive backlog as above

        // The floor applies, so the same modest lag is correctly tolerated as clean.
        assertThat(finder.getLo()).isEqualTo(100.0);
        assertThat(finder.getHi()).isNull();
    }

    @Test
    void backlogFloorDefaultsTo1000MessagesWhenUnset() {
        Workload workload = workload();
        workload.rampStartRate = 100;
        workload.rampMaxBacklogSeconds = 0.1; // limit would be only 10 messages without a floor
        workload.rampBracketHoldSeconds = 1; // one poll completes the hold for a clean verdict
        RampRateFinder finder = new RampRateFinder(workload); // rampMaxBacklogFloor left unset
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        // 800 messages of cumulative receive backlog -- below the default floor (1000) but would
        // exceed a smaller one (e.g. 500), pinning the default's exact value.
        finder.poll(periodNanos, 800, 0);

        assertThat(finder.getLo()).isEqualTo(100.0);
        assertThat(finder.getHi()).isNull();
    }

    @Test
    void safetyCapForcesCompletionWithBestKnownRate() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 10;
        workload.rampMaxDiscoveryMinutes = 1; // 60s cap
        RampRateFinder finder = new RampRateFinder(workload);

        // First poll consumes the (zero-length, per workload()) settle step.
        boolean done = finder.poll(SECONDS.toNanos(10), 10000, 10000);
        assertThat(done).isFalse();
        assertThat(finder.getLo()).isNull();

        // Second poll: one full 10s hold at 1000, clean -> lo=1000. Total elapsed 20s, within cap.
        done = finder.poll(SECONDS.toNanos(10), 20000, 20000);
        assertThat(done).isFalse();
        assertThat(finder.getLo()).isEqualTo(1000.0);

        // Third poll pushes total elapsed time past the 60s cap -- forces completion regardless.
        done = finder.poll(SECONDS.toNanos(45), 20000, 20000);

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.getCurrentRate()).isEqualTo(1000.0);
        // The safety cap is never a genuine confirm, even though it never contradicted anything.
        assertThat(finder.isConfirmed()).isFalse();
    }

    @Test
    void safetyCapDuringAPendingConfirmationHoldIsNotTreatedAsAConfirm() {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 3; // fast bracket setup
        // 21s (7 polls), not 30: because bracket holds for 3s where chop holds longer, chop now
        // re-verifies lo at full length before bisecting, adding one full hold to the sequence. The cap
        // has to land *inside* the confirmation hold for this test to be about anything, and
        // rampMaxDiscoveryMinutes is whole minutes, so the hold has to fit the 60s budget rather than
        // the budget fitting the hold. Sequence: 3s settle + 3s + 3s bracket + 21s re-verify + 21s chop
        // = 51s, leaving 9s of the 60s budget against a 21s confirmation hold.
        workload.rampHoldSeconds = 21;
        workload.rampConvergenceTolerance = 0.5;
        workload.rampMaxDiscoveryMinutes = 1; // 60s cap
        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(1800); // 1500 (candidate) stays clean, 2000 doesn't
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle

        // Bracket (2 polls) -> re-verify lo (7 polls) -> CHOP mid=1500. That hold (7 polls) passes and
        // is within tolerance -> confirming flips true. The confirmation hold then needs another 21s,
        // but the 60s safety cap fires 9s into it.
        boolean done = false;
        for (int i = 0; i < 45 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(finder.isConfirming()).isTrue();
        // The confirmation hold was still pending when the safety cap forced completion --
        // that must not be mistaken for a genuine confirm.
        assertThat(finder.isConfirmed()).isFalse();
    }

    @Test
    void aHoldShorterThanTheBrokerBurstBudgetWronglyAcceptsAnOversubscribedRate() {
        // Sustained capacity is 1000 msg/s, but the broker can absorb a 4000-message burst before
        // any backpressure shows (page cache / batching / socket buffers -- what FakeSystem's
        // instantaneous hard cap cannot model). At a 2000 msg/s candidate the burst buffer fills in
        // 4000 / (2000 - 1000) = 4 seconds, so a hold shorter than that sees only clean polls and
        // wrongly accepts 2000 msg/s as sustainable. This is the over-confirm the Kafka integration
        // test reproduced against a real bursting broker with short holds.
        Workload workload = workload();
        workload.rampStartRate = 2000;
        workload.rampBracketHoldSeconds = 3; // shorter than the 4s burst-fill time
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(1000, 4000);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        for (int i = 0; i < 3; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getLo()).isEqualTo(2000.0); // oversubscribed rate accepted as clean
    }

    @Test
    void aHoldLongerThanTheBurstBudgetCorrectlyRejectsTheOversubscribedRate() {
        // Identical bursty broker, but a hold longer than the 4s burst-fill time: once the buffer
        // saturates, publishes fall behind and the same 2000 msg/s candidate is correctly failed.
        // The only difference from the test above is rampBracketHoldSeconds -- proving hold length
        // is the lever, which no hold length could have revealed against FakeSystem's hard cap.
        Workload workload = workload();
        workload.rampStartRate = 2000;
        workload.rampBracketHoldSeconds = 6; // longer than the 4s burst-fill time
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(1000, 4000);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        boolean done = false;
        for (int i = 0; i < 6 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getHi()).isEqualTo(2000.0); // correctly failed once the burst drained
        assertThat(finder.getLo()).isNull(); // never accepted as a clean lo
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

    /**
     * A broker that absorbs a burst above its sustained capacity into a finite buffer before any
     * backpressure appears -- a leaky bucket, unlike {@link FakeSystem}'s instantaneous hard cap. An
     * oversubscribed rate therefore looks clean until the buffer saturates, so whether the rate
     * finder detects it depends entirely on how long the candidate is held. This is the property that
     * makes hold length matter, and the one the old hard-cap model could not express.
     */
    private static final class FakeBurstySystem {
        private final double sustainedCapacity;
        private final double burstBudget;
        private double buffer = 0;
        long totalPublished = 0;
        long totalReceived = 0;

        FakeBurstySystem(double sustainedCapacity, double burstBudget) {
            this.sustainedCapacity = sustainedCapacity;
            this.burstBudget = burstBudget;
        }

        void advance(double rate, long periodNanos) {
            double periodSeconds = periodNanos / 1e9;
            double arrived = rate * periodSeconds;
            double drained = sustainedCapacity * periodSeconds;
            // Each period the broker can publish what it drains plus whatever free buffer remains.
            double accepted = Math.min(arrived, drained + (burstBudget - buffer));
            buffer = Math.max(0, Math.min(burstBudget, buffer + accepted - drained));
            totalPublished += (long) accepted;
            totalReceived += (long) accepted;
        }
    }

    private static Workload throughputWorkload() {
        Workload workload = new Workload();
        workload.subscriptionsPerTopic = 1; // one delivery per publish, so received tracks published
        workload.rampVerdict = RampVerdict.THROUGHPUT;
        workload.rampMinThroughputRatio = 0.95;
        workload.rampSettleSeconds = 0;
        workload.rampBracketHoldSeconds = 1; // one 1s poll completes a hold
        workload.rampHoldSeconds = 1;
        workload.rampConvergenceTolerance = 0.05;
        workload.rampDrainSeconds = 0; // see workload() -- pinned off so trajectories stay readable
        return workload;
    }

    @Test
    void throughputVerdictConvergesNearProducerCapacity() {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        // Producer caps at 4500 msg/s; consumer never the bottleneck.
        FakeThroughputSystem system = new FakeThroughputSystem(4500, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.DONE);
        assertThat(Math.abs(4500.0 - finder.getCurrentRate()) / 4500.0).isLessThan(0.1);
    }

    @Test
    void throughputVerdictRejectsRatesWhereTheConsumerCannotKeepUp() {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        // Producer could do 100k, but the consumer only drains 3000 msg/s -> consumer is the ceiling.
        FakeThroughputSystem system = new FakeThroughputSystem(100_000, 3000);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        // Discovery is pinned by consumer drain (~3000), far below producer capacity.
        assertThat(finder.getCurrentRate()).isLessThan(6000.0);
        assertThat(finder.getCurrentRate()).isGreaterThan(1500.0);
    }

    @Test
    void throughputIgnoresLargeButStableInFlightBacklogThatTheCountVerdictRejects() {
        // A stable 50,000-message in-flight backlog is always present; the consumer keeps pace with
        // it (drain ratio ~1.0) but never closes it. Producer caps at 50,000 msg/s. THROUGHPUT
        // rides through the stable backlog to the real ~50k ceiling.
        Workload throughput = throughputWorkload();
        throughput.rampStartRate = 1000;
        RampRateFinder throughputFinder = new RampRateFinder(throughput);
        driveToCompletion(throughputFinder, new FakeStableBacklogSystem(50_000, 50_000));
        assertThat(Math.abs(50_000.0 - throughputFinder.getCurrentRate()) / 50_000.0).isLessThan(0.1);

        // The same broker under the default count-based BACKLOG verdict sees 50,000 >> its
        // 100-message limit at *every* rate, so no candidate ever holds: the bracket phase halves
        // all the way down and never establishes a lo at all. Asserting a loose upper bound here
        // (the original "< 25,000") passes for entirely the wrong reason -- it hides the collapse
        // rather than pinning it. WorkloadGeneratorRampTest covers the generator refusing to report
        // this outcome as a rate.
        Workload backlog = workload();
        backlog.rampStartRate = 1000;
        backlog.rampBracketHoldSeconds = 1;
        backlog.rampHoldSeconds = 1;
        RampRateFinder backlogFinder = new RampRateFinder(backlog);
        driveToCompletion(backlogFinder, new FakeStableBacklogSystem(50_000, 50_000));
        assertThat(backlogFinder.getLo()).as("no candidate ever held cleanly").isNull();
        assertThat(backlogFinder.getCurrentRate()).isLessThan(1.0);
    }

    @Test
    void throughputVerdictDoesNotConfirmAboveMeasuredCapacity() {
        // The ratio gate accepts while capacity >= ratio x rate, i.e. any rate up to
        // capacity / 0.95 -- so the accepted target can exceed everything the system ever actually
        // published. Because UniformRateLimiter hands out a fixed virtual schedule with no
        // catch-up, published can never exceed expected: the ratio is one-sided, so the 0.95 slack
        // is absorbed as *permanent shortfall*, not as jitter. A persistent shortfall is unbounded
        // publish-delay growth, which is exactly what ChopRateFinderKafkaIT calls unsustainable.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeThroughputSystem system = new FakeThroughputSystem(4500, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getCurrentRate())
                .as("a rate the producer never reached is not a sustainable rate")
                .isLessThanOrEqualTo(4500.0);
    }

    @Test
    void throughputVerdictAlsoOverConfirmsWhenTheHoldIsShorterThanTheBurstBudget() {
        // THROUGHPUT is scale-free but not hold-length-immune. While the broker's burst buffer still
        // has room, published tracks expected exactly, so achievedRatio reads 1.000 and an
        // oversubscribed rate looks perfectly clean. Sustained capacity 1000 msg/s with a
        // 4000-message burst budget fills in 4000 / (2000 - 1000) = 4s, so a 3s hold never sees the
        // shortfall. The design doc asserts this limitation but nothing exercised it: every other
        // THROUGHPUT fixture is an instantaneous hard cap, under which the predicate is monotone by
        // construction and hold length cannot matter.
        //
        // This is also the limit of the second-half-vs-first-half trend check, and the reason that
        // check narrows hold-length sensitivity rather than removing it. The buffer here absorbs for
        // the whole hold, so both halves publish a flat 2000 msg/s and the trend ratio is 1.000. No
        // statistic computed inside this hold can distinguish it from a healthy one -- only a longer
        // hold can, which is what the next test does.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 2000;
        workload.rampBracketHoldSeconds = 3; // shorter than the 4s burst-fill time
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(1000, 4000);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        for (int i = 0; i < 3; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getLo()).isEqualTo(2000.0); // oversubscribed rate accepted as clean
    }

    @Test
    void throughputVerdictRejectsTheOversubscribedRateOnceTheHoldOutlastsTheBurst() {
        // Same bursty broker, hold longer than the 4s burst-fill time: the buffer saturates, the
        // last two seconds only manage 1000 msg/s, and the hold's aggregate lands at
        // 10000 / 12000 = 0.833 -- below the 0.95 gate, so the candidate is correctly failed. Hold
        // length is the only difference from the test above.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 2000;
        workload.rampBracketHoldSeconds = 6; // longer than the 4s burst-fill time
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(1000, 4000);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        boolean done = false;
        for (int i = 0; i < 6 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getHi()).isEqualTo(2000.0);
        assertThat(finder.getLo()).isNull();
    }

    @ParameterizedTest
    @ValueSource(ints = {25, 45, 90})
    void aHoldWhoseThroughputDeclinesIsRejectedEvenWhenItsAggregateRatioPasses(int holdSeconds) {
        // The gap the aggregate ratio cannot close. Sustained capacity is 1000 msg/s and the burst
        // budget is sized so the buffer saturates 92% of the way through the hold. Up to that point
        // published tracks expected exactly, so over the hold as a whole the producer still delivers
        // 48000 of an expected 50000 -- ratio 0.96, comfortably past the 0.95 gate. The candidate is
        // accepted, and it is not sustainable: the last two seconds manage 1000 msg/s, which is what
        // the *next* 60-second measurement window would run into for its whole duration.
        //
        // Averaging over the whole hold is what hides this: a decline confined to the tail is diluted
        // by every healthy second before it. Comparing the hold's second half against its first keeps
        // the decline undiluted -- 1846 vs 2000 msg/s, a 0.923 ratio, below the same 0.95 gate.
        //
        // Parameterised over hold length because the *point* is that the verdict no longer depends on
        // it: the burst budget scales with the hold, so all three see saturation at the same 92% mark
        // and all three must reject. Under the aggregate alone, all three accept.
        double sustainedCapacity = 1000;
        double rate = 2000;
        // (rate - capacity) x 0.92 x holdSeconds: the overshoot absorbed before saturation.
        double burstBudget = (rate - sustainedCapacity) * 0.92 * holdSeconds;

        Workload workload = throughputWorkload();
        workload.rampStartRate = (int) rate;
        workload.rampBracketHoldSeconds = holdSeconds;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(sustainedCapacity, burstBudget);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        for (int i = 0; i < holdSeconds; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getHi())
                .as("declining throughput within the hold must fail the candidate")
                .isEqualTo(rate);
        assertThat(finder.getLo()).isNull();
    }

    @Test
    void aHoldWithSteadyThroughputIsNotRejectedByTheTrendCheck() {
        // The other side of the gate: a hold that is genuinely flat must not be failed by it. Same
        // 25-second hold, but the candidate sits below capacity, so both halves deliver the full rate.
        // Without this, the trend check could reject every candidate and the search would collapse.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 25;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeThroughputSystem system = new FakeThroughputSystem(50_000, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        for (int i = 0; i < 25; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(finder.getLo()).isEqualTo(1000.0);
        assertThat(finder.getHi()).isNull();
    }

    @Test
    void aHoldTooShortToSplitInHalfIsJudgedOnItsAggregateAlone() {
        // A hold of a single poll has no second half to compare against, so the trend check has to
        // abstain rather than guess. This is why the one-poll holds the other tests use are unaffected
        // by the gate -- and it is a real limit, not just a test convenience: the shorter the hold, the
        // less the trend check has to work with.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 2000;
        workload.rampBracketHoldSeconds = 1; // one 1s poll completes the hold
        RampRateFinder finder = new RampRateFinder(workload);
        FakeBurstySystem system = new FakeBurstySystem(1000, 4000);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle
        system.advance(finder.getCurrentRate(), periodNanos);
        finder.poll(periodNanos, system.totalPublished, system.totalReceived);

        // Absorbed entirely by the burst budget, so the aggregate sees a clean 2000 of 2000.
        assertThat(finder.getLo()).isEqualTo(2000.0);
    }

    /** A producer/consumer pair with independent sustained capacities, seeded from zero. */
    private static final class FakeThroughputSystem {
        private final double producerCapacity;
        private final double consumerCapacity;
        long totalPublished;
        long totalReceived;

        FakeThroughputSystem(double producerCapacity, double consumerCapacity) {
            this.producerCapacity = producerCapacity;
            this.consumerCapacity = consumerCapacity;
            this.totalPublished = 0;
            this.totalReceived = 0;
        }

        void advance(double rate, long periodNanos) {
            double periodSeconds = periodNanos / 1e9;
            totalPublished += (long) (Math.min(rate, producerCapacity) * periodSeconds);
            long lag = totalPublished - totalReceived;
            totalReceived += (long) Math.min(consumerCapacity * periodSeconds, lag);
        }
    }

    /**
     * A producer capped at {@code producerCapacity} behind an already-established, stable in-flight
     * backlog: the consumer keeps pace (drain ratio ~1.0) but never closes the constant lag, so
     * receiveBacklog stays large-but-constant -- the case an absolute-count verdict mishandles.
     */
    private static final class FakeStableBacklogSystem {
        private final double producerCapacity;
        private final long standingBacklog;
        long totalPublished;
        long totalReceived;

        FakeStableBacklogSystem(double producerCapacity, long standingBacklog) {
            this.producerCapacity = producerCapacity;
            this.standingBacklog = standingBacklog;
            this.totalPublished = standingBacklog; // pre-established, stable in-flight backlog
            this.totalReceived = 0;
        }

        void advance(double rate, long periodNanos) {
            totalPublished += (long) (Math.min(rate, producerCapacity) * (periodNanos / 1e9));
            totalReceived = totalPublished - standingBacklog; // consumer keeps pace; lag stays constant
        }
    }

    private static void driveToCompletion(RampRateFinder finder, FakeStableBacklogSystem system) {
        long periodNanos = SECONDS.toNanos(1);
        boolean done = false;
        for (int i = 0; i < 500 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }
        assertThat(done).isTrue();
    }

    @Test
    void anOvershootIsDrainedAtAKnownGoodRateBeforeTheNextCandidateIsJudged() {
        // A failed candidate leaves the system carrying its overshoot -- deep queues, consumer lag,
        // GC pressure. Judging the next candidate immediately measures that fallout rather than the
        // candidate, and since bracket only ever moves lo upward, one contaminated reading is
        // unrecoverable. So after a failure, run at a rate already known to be sustainable (lo)
        // until the backlog clears, and evaluate nothing while doing it.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampDrainSeconds = 5;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        // 1000 holds clean, so lo = 1000 -- the rate the drain will later fall back to.
        finder.poll(periodNanos, 1000, 1000);
        assertThat(finder.getLo()).isEqualTo(1000.0);
        assertThat(finder.isDraining()).isFalse();

        // 2000 breaches (500 messages of receive backlog against the 100 limit) -> overshoot.
        finder.poll(periodNanos, 3000, 2500);
        assertThat(finder.getHi()).isEqualTo(2000.0);
        assertThat(finder.isDraining()).as("a failure must trigger recovery").isTrue();
        assertThat(finder.getCurrentRate())
                .as("drain runs below the known-good lo, so there is headroom to clear the queue")
                .isEqualTo(500.0);

        // While draining, nothing is judged: the backlog still present is the overshoot's, not the
        // next candidate's, so no verdict may be recorded from it.
        finder.poll(periodNanos, 4000, 3000);
        assertThat(finder.isDraining()).isTrue();
        assertThat(finder.getHi()).isEqualTo(2000.0); // unchanged -- no new verdict
    }

    @Test
    void thereIsNoDrainWhileBracketIsStillHalvingDownwardWithNoKnownGoodRate() {
        // The drain's whole premise is that it runs at a rate already known to be sustainable, so
        // queues
        // actually shrink -- draining at the *next* candidate guarantees nothing, because that
        // candidate
        // may itself be above capacity. On bracket's downward path there is no such rate yet: lo is
        // null. Draining there is worse than not draining, because the recovery test never passes, the
        // full cap is burned, and the halved candidate then starts its hold with a *larger* backlog
        // than
        // if the search had simply moved on. So skip it and halve immediately.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampDrainSeconds = 5;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        // The start rate itself is already overloaded: 900 messages of receive backlog against the
        // 100 limit, and the hold ends while still breaching, so it fails with no lo ever established.
        finder.poll(periodNanos, 1000, 100);

        assertThat(finder.getHi()).isEqualTo(1000.0);
        assertThat(finder.getLo()).isNull();
        assertThat(finder.isDraining())
                .as("no known-good rate to drain at, so there is nothing to drain toward")
                .isFalse();
        assertThat(finder.getCurrentRate()).as("halves straight away").isEqualTo(500.0);
    }

    @Test
    void recoveryWaitsForTheProducerToCatchUpAndNotJustTheConsumer() {
        // The drain's blind spot, the mirror of the acknowledgement-stall one. Its recovery test looked
        // only at cumulative receive backlog, and messages still queued in the producer client have not
        // been published yet -- so they contribute no receive backlog at all. With a 64MB buffer.memory
        // that is hundreds of thousands of messages the test cannot see, and every drain in the local
        // Kafka run duly reported "recovery complete after 2s" while publish delay was still seconds
        // deep. The next candidate then measured that fallout as its own.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampDrainSeconds = 600; // far longer than this test needs
        workload.rampMaxBacklogSeconds = 0.5; // also the producer's catch-up tolerance: 0.5s
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1000, 1000, 0); // 1000 holds clean -> lo = 1000
        assertThat(finder.getLo()).isEqualTo(1000.0);
        // 2000 publishes fully but leaves 1,500 messages of receive backlog against the 1,000 limit
        // (the floor, since 0.5s at 1000 msg/s is only 500), and the hold ends while still breaching.
        finder.poll(periodNanos, 3000, 1500, 0);
        assertThat(finder.isDraining()).isTrue();

        // Backlog is clear, but the producer is still 3 seconds behind its own schedule: the overshoot
        // is sitting in its send buffer, invisible to a receive-backlog check.
        finder.poll(periodNanos, 4000, 4000, SECONDS.toMicros(3));
        assertThat(finder.isDraining())
                .as("consumer caught up, producer has not -- not recovered")
                .isTrue();

        // Delay subsides below the 0.5s tolerance. Now it is genuinely recovered.
        finder.poll(periodNanos, 5000, 5000, 100_000);

        assertThat(finder.isDraining()).isFalse();
        assertThat(finder.getCurrentRate()).isEqualTo(1500.0);
    }

    @Test
    void recoveryRunsWithHeadroomBecauseDrainingAtLoNeverCatchesUp() {
        // Recovery used to run at lo itself, on the reasoning that lo is a rate known to be
        // sustainable. That is true and insufficient: lo means "keeps up", not "has spare capacity".
        // Clearing a queue needs arrival below service, and at lo there is almost none, so the queue
        // drains at (capacity - lo) -- nearly zero. On AKS two of four recoveries ran their full 180s
        // cap and gave up with the producer still 10 and 24 seconds behind its schedule, at a drain
        // rate of ~1.16M against a ceiling of ~1.17M. Half of lo leaves real headroom, and since
        // recovery is capped and nothing is evaluated during it, running slower costs nothing.
        Workload workload = workload();
        workload.rampStartRate = 100000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampDrainSeconds = 600;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 100_000, 100_000); // 100k holds clean -> lo = 100,000
        assertThat(finder.getLo()).isEqualTo(100000.0);
        finder.poll(periodNanos, 300_000, 299_000); // 200k leaves 1,000 backlog vs the 100 limit

        assertThat(finder.isDraining()).isTrue();
        assertThat(finder.getCurrentRate())
                .as("half of lo: enough headroom that a queue actually shrinks")
                .isEqualTo(50000.0);
    }

    @Test
    void drainEndsEarlyOnceTheBacklogHasCleared() {
        // The drain is capped, not fixed: waiting out the full cap when the system already recovered
        // just burns the discovery budget. Once the backlog is back within the limit it is judged
        // against, carry on.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampDrainSeconds = 600; // far longer than this test will need
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0);
        finder.poll(periodNanos, 1000, 1000);
        finder.poll(periodNanos, 3000, 2500); // breach -> drain
        assertThat(finder.isDraining()).isTrue();

        // Consumers catch up: backlog back to 0, inside the 100-message limit.
        finder.poll(periodNanos, 4000, 4000);

        assertThat(finder.isDraining())
                .as("recovered well inside the 600s cap, so the drain should not wait it out")
                .isFalse();
        assertThat(finder.getCurrentRate())
                .as("resumes the candidate the search had queued up, not the drain rate")
                .isEqualTo(1500.0);
    }

    // Pairs with one-second polls so a single poll completes a hold and every candidate's verdict is
    // decided by exactly the counters that poll is handed. Backlog limits are the 100 messages
    // workload() sets, and convergenceTolerance is left at its 0.05 default.
    private static Workload seedWorkload(boolean seedFromAchievedRate) {
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 1;
        workload.rampSeedFromAchievedRate = seedFromAchievedRate;
        return workload;
    }

    @Test
    void aFailedCandidateSeedsTheNextOneFromWhatItActuallyAchieved() {
        // A candidate that fails has already measured the system: it asked for 2000 msg/s and got
        // 1200, so 1200 is a direct capacity estimate. Bisecting to the midpoint of [1000, 2000]
        // discards that measurement and spends another full hold rediscovering it. Both finders here
        // are handed identical counters and differ only in the flag, so the midpoint and the seed are
        // directly comparable.
        RampRateFinder seeded = new RampRateFinder(seedWorkload(true));
        RampRateFinder bisecting = new RampRateFinder(seedWorkload(false));
        long periodNanos = SECONDS.toNanos(1);

        for (RampRateFinder finder : List.of(seeded, bisecting)) {
            finder.poll(periodNanos, 0, 0); // settle + baseline
            // 1000 msg/s holds clean -> lo = 1000, bracket doubles to 2000.
            finder.poll(periodNanos, 1000, 1000);
            assertThat(finder.getLo()).isEqualTo(1000.0);
            // 2000 msg/s publishes only 1200 of its expected 2000: an 800-message publish backlog
            // against the 100 limit, so the candidate fails and hi = 2000.
            finder.poll(periodNanos, 2200, 2200);
            assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
            assertThat(finder.getHi()).isEqualTo(2000.0);
        }

        assertThat(bisecting.getCurrentRate()).as("blind midpoint of [1000, 2000]").isEqualTo(1500.0);
        assertThat(seeded.getCurrentRate())
                .as("the throughput the failed hold actually achieved")
                .isEqualTo(1200.0);
    }

    @Test
    void aCandidateThatFailedWhilePublishingAtFullRateIsNotUsedAsACapacityEstimate() {
        // The guard that matters most. Here 2000 msg/s publishes all 2000 and fails purely on consumer
        // lag, so the achieved rate equals the target and says nothing about capacity. Seeding from it
        // would propose the rate that just failed; clamping that back inside the bracket would creep
        // downward by a tolerance-width per hold instead of halving, which is *slower* than bisection
        // and is exactly the shape of a real THROUGHPUT run where a broker absorbed the overshoot into
        // its buffers and reported achievedRatio 1.000 at every rate. So when the estimate is not
        // meaningfully below the rate that failed, bisect.
        RampRateFinder finder = new RampRateFinder(seedWorkload(true));
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1000, 1000); // 1000 clean -> lo = 1000, next 2000
        // All 2000 published, but only 1500 of the 3000 cumulative deliveries drained: a 500-message
        // receive backlog fails the candidate with the producer perfectly healthy.
        finder.poll(periodNanos, 3000, 2500);

        assertThat(finder.getHi()).isEqualTo(2000.0);
        assertThat(finder.getCurrentRate())
                .as("achieved == target carries no capacity information, so fall back to the midpoint")
                .isEqualTo(1500.0);
    }

    @Test
    void aSeedAtOrBelowAKnownGoodRateIsRejectedInFavourOfTheMidpoint() {
        // 1000 msg/s is already known to hold. If a later candidate's achieved rate comes back *below*
        // that -- a throttled producer, a stalled partition -- seeding from it would re-test ground the
        // search has already covered and abandon the bracket it paid for. The bracket is still valid
        // evidence, so bisect it.
        RampRateFinder finder = new RampRateFinder(seedWorkload(true));
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1000, 1000); // 1000 clean -> lo = 1000, next 2000
        finder.poll(periodNanos, 3000, 2500); // consumer-lag failure -> hi = 2000, midpoint 1500
        assertThat(finder.getCurrentRate()).isEqualTo(1500.0);

        // 1500 publishes only 800 -- an achieved rate below the known-good lo of 1000.
        finder.poll(periodNanos, 3800, 3800);

        assertThat(finder.getHi()).isEqualTo(1500.0);
        assertThat(finder.getCurrentRate())
                .as("800 is below the known-good lo, so bisect [1000, 1500] instead")
                .isEqualTo(1250.0);
    }

    // Drives a fixed-capacity fixture to completion and returns the polls discovery cost, checking on
    // the way out that it still lands on the real ceiling. Mirrors the integration test's geometry:
    // 3s
    // polls, 45s holds, and a deliberately low 5000 msg/s start rate, so the bracket phase does real
    // work climbing to the ceiling rather than starting next to it.
    private static int pollsToDiscover(double capacity, boolean seedFromAchievedRate) {
        Workload workload = throughputWorkload();
        workload.rampStartRate = 5000;
        workload.rampBracketHoldSeconds = 45;
        workload.rampHoldSeconds = 45;
        workload.rampMaxDiscoveryMinutes =
                600; // measuring search cost, so the safety cap must not bite
        workload.rampSeedFromAchievedRate = seedFromAchievedRate;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeThroughputSystem system = new FakeThroughputSystem(capacity, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(3);

        int polls = 0;
        boolean done = false;
        while (!done && polls < 10_000) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
            polls++;
        }
        assertThat(done).isTrue();
        assertThat(Math.abs(capacity - finder.getCurrentRate()) / capacity)
                .as("seeded or not, discovery must still land on the real ceiling")
                .isLessThan(0.05);
        return polls;
    }

    @Test
    void seedingReachesTheSameCeilingInFewerHoldsThanBlindBisection() {
        // The reason the seed exists: discovery time. Every hold costs its full length under
        // THROUGHPUT, which has no per-poll fast-fail, so holds are the unit of cost. Note this is a
        // real but modest saving -- the seed places lo accurately and leaves hi where bracket's last
        // doubling put it, so chop still has the whole [lo, hi] span to bisect afterwards.
        int bisecting = pollsToDiscover(841_000, false);
        int seeded = pollsToDiscover(841_000, true);

        assertThat(seeded).isLessThan(bisecting);
    }

    @Test
    void seedingDoesNotHelpWhenTheStartRateIsAlreadyAboveCapacity() {
        // A deliberate scope boundary. When the start rate is above capacity the bracket phase halves
        // *downward*, and that path is not seeded: halving is already geometric, and a single spurious
        // low reading would otherwise drop the search orders of magnitude in one step with no way back
        // up (nothing ever reopens hi). So this geometry costs exactly what it did before, and the
        // near-zero-rate collapse the halving path can produce is a separate problem, guarded by
        // WorkloadGenerator refusing to report a rate when no candidate ever held.
        int bisecting = pollsToDiscover(4500, false);
        int seeded = pollsToDiscover(4500, true);

        assertThat(seeded).isEqualTo(bisecting);
    }

    @Test
    void seedingStillConvergesOnTheSameCapacityAsBlindBisection() {
        // The seed changes the trajectory, so the guarantee that matters is that it does not change the
        // destination: both must still land on the producer's real ceiling.
        Workload workload = throughputWorkload();
        workload.rampStartRate = 1000;
        workload.rampSeedFromAchievedRate = true;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeThroughputSystem system = new FakeThroughputSystem(4500, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.isConfirmed()).isTrue();
        assertThat(Math.abs(4500.0 - finder.getCurrentRate()) / 4500.0).isLessThan(0.1);
    }

    @Test
    void aSingleBreachingPollDoesNotFailACandidateThatIsOtherwiseKeepingUp() {
        // From the AKS trial: a 640,000 msg/s candidate ran 7 consecutive healthy polls (achieved
        // 629,694-650,592, backlog 3,430-6,573 against a 100,000 limit), then one 3-second poll dipped
        // to 603,315 -- a publish shortfall of 110,055, exceeding the limit by 10%. That single sample
        // set hi=640,000 permanently, because nothing ever reopens hi. The hold's own aggregate was
        // published/expected = 99.13%.
        //
        // A per-poll fast-fail has no noise tolerance at all: one sample decides. Real overload is not
        // like that -- the same trial measured backlog going from ~1,300 to ~74,800 in one poll and
        // then *staying* elevated for 12-15s. So requiring two consecutive breaches costs one poll
        // against a genuine failure and filters a transient entirely.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 15; // 5 polls, so the breach is not the final one
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        // Three clean polls at 1000 msg/s: 3000 published per period, consumers keeping pace.
        finder.poll(periodNanos, 3000, 3000);
        finder.poll(periodNanos, 6000, 6000);
        finder.poll(periodNanos, 9000, 9000);
        assertThat(finder.getHi()).isNull();

        // One poll breaches: 500 messages of receive backlog against the 100 limit. Isolated.
        finder.poll(periodNanos, 12000, 11500);
        assertThat(finder.getHi()).as("a lone breaching poll must not condemn the rate").isNull();

        // Recovers on the next poll, which also completes the 15s hold. A hold that *ended* while
        // still breaching is a different case and does fail -- the tolerance is for breaches the
        // candidate recovered from, not one still in progress when the clock runs out.
        finder.poll(periodNanos, 15000, 15000);
        assertThat(finder.getHi()).isNull();
        assertThat(finder.getLo()).as("the candidate held: 15s of hold completed").isEqualTo(1000.0);
    }

    @Test
    void twoConsecutiveBreachingPollsStillFailTheCandidateImmediately() {
        // The other side: sustained overload must still fail fast. Debouncing buys noise immunity, and
        // it must not turn into waiting out a full hold on a rate that is genuinely gone.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 60; // long, so an early exit is unambiguous
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(3);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 3000, 3000); // clean
        finder.poll(periodNanos, 6000, 5500); // breach 1: 500 behind
        assertThat(finder.getHi()).isNull();
        finder.poll(periodNanos, 9000, 8000); // breach 2: 1000 behind, consecutive

        assertThat(finder.getHi())
                .as("sustained overload fails on the second breach, not after the full 60s hold")
                .isEqualTo(1000.0);
    }

    @Test
    void aCheapBracketProbeHasItsLoReVerifiedAtFullLengthBeforeChopping() {
        // The speed lever. Bracket's early doublings are pure overhead on a fast cluster: the AKS run
        // spent 6 x 89s climbing 10k -> 320k with backlog flat at a few thousand the whole way,
        // learning
        // nothing but "still fine". Shortening rampBracketHoldSeconds reclaims that, but on its own it
        // is unsafe -- a short hold can miss slow-building absorption, and lo only ever moves *up*, so
        // an over-confirmed lo can never be undone.
        //
        // So when bracket ran cheaper than chop, the first thing chop does is re-run lo at full length.
        // The probe locates the bracket; the full hold is what certifies it.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1; // cheap probe
        workload.rampHoldSeconds = 3; // full rigor
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1000, 1000); // 1000 probes clean -> lo = 1000, doubles to 2000
        assertThat(finder.getLo()).isEqualTo(1000.0);
        finder.poll(periodNanos, 3000, 2500); // 2000 breaches on the probe's only poll -> hi = 2000

        assertThat(finder.getPhase()).isEqualTo(RampRateFinder.Phase.CHOP);
        assertThat(finder.getCurrentRate())
                .as("re-verify the cheaply-probed lo at full length, rather than bisecting from it")
                .isEqualTo(1000.0);
    }

    @Test
    void aFailedReVerificationRepositionsTheBracketInsteadOfStalling() {
        // What the re-verification is *for*: catching a lo the cheap probe over-confirmed. When it
        // fails, hi becomes that lo -- so lo and hi collide, and bisecting would retest the same rate
        // forever. The search has to fall back to a lower recorded pass.
        //
        // And this must not latch isNonMonotonic(). A 1s probe passing where a 3s hold fails is not the
        // system contradicting itself; it is two measurements of different rigor, which is the whole
        // premise of probing cheaply. Latching here would withhold every result from every run that
        // used a cheap bracket, making the option useless.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        workload.rampHoldSeconds = 3;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 1000, 1000); // probe passes 1000
        finder.poll(periodNanos, 3000, 2500); // 2000 fails -> CHOP, re-verifying 1000
        assertThat(finder.getCurrentRate()).isEqualTo(1000.0);

        // 1000 now fails its full-length hold: two consecutive breaching polls.
        finder.poll(periodNanos, 4000, 3000); // 1000 behind
        finder.poll(periodNanos, 5000, 3500); // 1500 behind -- consecutive, so the candidate fails

        assertThat(finder.getHi())
                .as("the over-confirmed lo becomes the new ceiling")
                .isEqualTo(1000.0);
        assertThat(finder.getLo())
                .as("falls back below it rather than colliding with hi and stalling")
                .isLessThan(1000.0);
        assertThat(finder.getCurrentRate()).isLessThan(1000.0);
        assertThat(finder.isNonMonotonic())
                .as("a cheap probe disagreeing with a full hold is not system instability")
                .isFalse();
    }

    // Drives the AKS run's geometry -- 5,000 msg/s start, 180s chop holds, 3s polls, seeding on --
    // against a hard 589,000 msg/s ceiling, and returns {discovery seconds, confirmed rate}.
    private static double[] discoverAtBracketHold(int bracketHoldSeconds) {
        Workload workload = new Workload();
        workload.subscriptionsPerTopic = 1;
        workload.rampStartRate = 5000;
        workload.rampMaxBacklogSeconds = 0.5;
        workload.rampBracketHoldSeconds = bracketHoldSeconds;
        workload.rampHoldSeconds = 180;
        workload.rampConvergenceTolerance = 0.05;
        workload.rampSeedFromAchievedRate = true;
        workload.rampSettleSeconds = 30;
        workload.rampMaxDiscoveryMinutes = 600; // measuring search cost, so the cap must not bite

        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(589_000);
        long periodNanos = SECONDS.toNanos(3);
        int polls = 0;
        boolean done = false;
        while (!done && polls < 100_000) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
            polls++;
        }
        assertThat(done).isTrue();
        return new double[] {polls * 3.0, finder.getCurrentRate()};
    }

    @Test
    void aCheaperBracketProbeCutsDiscoveryTimeWithoutMovingTheAnswer() {
        // The payoff, and the reason the re-verification above is worth its one extra hold. The AKS run
        // used rampBracketHoldSeconds: 90 against 180s chop holds and spent 534s of its 1,606s climbing
        // through doublings whose backlog never moved. Shortening the probe reclaims most of that, and
        // because lo is re-verified at full length before chop, it does not cost accuracy.
        double[] slow = discoverAtBracketHold(90);
        double[] fast = discoverAtBracketHold(20);

        assertThat(fast[0]).as("a cheaper probe must actually be cheaper").isLessThan(slow[0]);
        assertThat(fast[1])
                .as("and must land on the same rate -- the saving is in probing, not in rigor")
                .isEqualTo(slow[1]);
    }

    @Test
    void anAcknowledgementStallIsNotEvidenceAboutTheRate() {
        // AKS, transparent arm: two polls in which nothing acknowledged failed a re-verification of a
        // rate that had just held cleanly, and the search converged on 9,203 msg/s against a real
        // ceiling near 1,100,000 -- about 120x low. Every counter and histogram the finder sees is
        // populated on ack, so an acknowledgement stall zeroes all of them at once while the messages
        // are still in flight. The deferred acks arrived on the very next poll: 41,278 msg/s against a
        // 5,000 target, with a p99 publish latency of 10.7 seconds.
        //
        // Nothing acknowledged is an absence of data, not a measurement of capacity. It says nothing
        // about whether the rate is too high, so it must not fail the candidate -- and it must not
        // count
        // toward the hold either, because a hold interrupted mid-window is not the window it claims.
        Workload workload = workload();
        workload.rampStartRate = 10000;
        workload.rampBracketHoldSeconds = 3;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 10_000, 10_000); // a real, clean poll: 1s of the 3s hold

        // Acks stop dead. Cumulative totals do not move, so published is exactly 0 for two polls --
        // which under a plain consecutive-breach check is two breaches and a failed candidate.
        finder.poll(periodNanos, 10_000, 10_000);
        finder.poll(periodNanos, 10_000, 10_000);
        assertThat(finder.getHi()).as("nothing acknowledged is not a breach").isNull();
        assertThat(finder.getLo()).as("nor is it a clean hold").isNull();

        // Acks resume. The hold restarts rather than resuming a window it did not observe, so three
        // fresh clean seconds are needed before the candidate is accepted.
        finder.poll(periodNanos, 20_000, 20_000);
        finder.poll(periodNanos, 30_000, 30_000);
        assertThat(finder.getLo()).as("hold restarted, so not yet complete").isNull();
        finder.poll(periodNanos, 40_000, 40_000);

        assertThat(finder.getLo()).as("three clean seconds after the stall").isEqualTo(10000.0);
        assertThat(finder.getHi()).isNull();
    }

    @Test
    void aSustainedPublishShortfallBreachesEvenWhileEachPollLooksTolerable() {
        // rampMaxBacklogSeconds used to be compared against two quantities of different dimension:
        // receiveBacklog is cumulative, so "rate x seconds" reads as "consumers are this many seconds
        // behind", but publishBacklog was this poll's shortfall alone. Against the same limit at a 1s
        // poll, 0.5 permitted a 50% shortfall *every poll, indefinitely*, because nothing accumulated.
        //
        // Here the producer manages 6,000 of an expected 10,000 forever. Per-poll that is a 4,000
        // shortfall against a 5,000 limit -- tolerable, every single time. As a fraction of what was
        // asked for it is 0.6, and no amount of patience makes that sustainable.
        Workload workload = workload();
        workload.rampStartRate = 10000;
        workload.rampMaxBacklogSeconds = 0.5; // 5,000 messages at this rate
        workload.rampBracketHoldSeconds = 60; // long, so only a breach can end this early
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        finder.poll(periodNanos, 6_000, 6_000);
        finder.poll(periodNanos, 12_000, 12_000);

        assertThat(finder.getHi())
                .as("60% of target is not sustainable however tolerable each poll looks")
                .isEqualTo(10000.0);
    }

    @Test
    void aGenuineShortfallStillBreachesEvenThoughAZeroDoesNot() {
        // The guard on the above: "published nothing" is excused, "published far too little" is not.
        // Otherwise the fix would blind the producer-side check entirely.
        Workload workload = workload();
        workload.rampStartRate = 10000;
        RampRateFinder finder = new RampRateFinder(workload);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, 0, 0); // settle + baseline
        // 100 of an expected 10,000: a 9,900-message publish shortfall against the 100 limit.
        finder.poll(periodNanos, 100, 100);
        finder.poll(periodNanos, 200, 200);

        assertThat(finder.getHi()).as("a measured shortfall is still a breach").isEqualTo(10000.0);
    }

    @Test
    void theDefaultDiscoveryBudgetCoversASearchAtTheDefaultHold() {
        // The two defaults have to stay coherent with each other. A hold long enough to outlast a real
        // broker's burst absorption makes discovery expensive, and under THROUGHPUT there is no
        // per-poll fast-fail, so *every* candidate costs its full hold -- including the doomed bracket
        // overshoots. If the time budget does not cover a realistic search at the default hold,
        // discovery truncates and reports its best known rate having never confirmed one, which from
        // the outside differs from success only by a WARN and an absent rampVerification.
        //
        // Nothing else pins this pair, so raising one without the other is a silent regression. This is
        // the test for it: everything below is left unset so the search runs on the shipped defaults.
        Workload workload = new Workload();
        workload.subscriptionsPerTopic = 1;
        workload.rampVerdict = RampVerdict.THROUGHPUT; // the expensive mode: no fast-fail
        RampRateFinder finder = new RampRateFinder(workload);
        FakeThroughputSystem system = new FakeThroughputSystem(1_000_000, 1_000_000_000L);
        long periodNanos = SECONDS.toNanos(3);

        boolean done = false;
        for (int i = 0; i < 5_000 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.isSafetyCapped())
                .as("the default budget must not truncate a search at the default hold")
                .isFalse();
        assertThat(finder.isConfirmed()).isTrue();
    }

    @Test
    void safetyCapIsDistinguishableFromAGenuineConfirm() {
        // Running out of time budget reports the best rate found so far, which from the outside is
        // indistinguishable from a verified one: isConfirmed() is false either way when a hold was
        // still pending, and getCurrentRate() returns a number regardless. A truncated discovery
        // needs to say so, or it gets read as a completed one.
        Workload workload = workload();
        workload.rampStartRate = 1000;
        workload.rampHoldSeconds = 3;
        workload.rampMaxDiscoveryMinutes = 1;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeSystem system = new FakeSystem(4500);
        long periodNanos = SECONDS.toNanos(10);

        boolean done = false;
        for (int i = 0; i < 10 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.isSafetyCapped()).isTrue();
        assertThat(finder.isConfirmed()).isFalse();
        assertThat(finder.getLo()).isEqualTo(4000.0); // truncated, but with a real answer to report
    }

    @Test
    void aGenuineConfirmIsNotFlaggedAsSafetyCapped() {
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

        assertThat(finder.isConfirmed()).isTrue();
        assertThat(finder.isSafetyCapped()).isFalse();
    }

    @Test
    void consumerDrainIsMeasuredPerSubscriptionNotPerPublish() {
        // Every publish is delivered once per subscription, so with 3 subscriptions the consumers
        // must move 3x the publish rate. Aggregate drain caps at 3000 deliveries/s, which pins the
        // sustainable *publish* rate at 3000 / 3 = 1000 msg/s -- the producer itself could do 100k.
        // Comparing received against published rather than against subscriptions x published makes
        // the drain look 3x healthier than it is, and discovery climbs to ~3x the real ceiling.
        Workload workload = throughputWorkload();
        workload.subscriptionsPerTopic = 3;
        workload.rampStartRate = 500;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeFanOutSystem system = new FakeFanOutSystem(100_000, 3000, 3);
        long periodNanos = SECONDS.toNanos(1);

        boolean done = false;
        for (int i = 0; i < 200 && !done; i++) {
            system.advance(finder.getCurrentRate(), periodNanos);
            done = finder.poll(periodNanos, system.totalPublished, system.totalReceived);
        }

        assertThat(done).isTrue();
        assertThat(finder.getCurrentRate())
                .as("sustainable publish rate is aggregate drain / subscriptions = 3000 / 3")
                .isCloseTo(1000.0, within(150.0));
    }

    @Test
    void receiveBacklogCountsDeliveriesPerSubscription() {
        // A standing 150-delivery backlog across 3 subscriptions exceeds the 100-message limit and
        // must fail the candidate. Subtracting raw received from raw published, rather than from
        // subscriptions x published, turns that genuine backlog into a large negative number -- so
        // the receive gate can never fire at all once there is more than one subscription.
        Workload workload = workload();
        workload.subscriptionsPerTopic = 3;
        workload.rampStartRate = 1000;
        workload.rampBracketHoldSeconds = 1;
        RampRateFinder finder = new RampRateFinder(workload);
        FakeFanOutStandingBacklogSystem system = new FakeFanOutStandingBacklogSystem(3, 150);
        long periodNanos = SECONDS.toNanos(1);

        finder.poll(periodNanos, system.totalPublished, system.totalReceived); // settle + baseline
        system.advance(finder.getCurrentRate(), periodNanos);
        finder.poll(periodNanos, system.totalPublished, system.totalReceived);

        assertThat(finder.getHi())
                .as("3 x 1000 published - 2850 received = 150 > 100")
                .isEqualTo(1000.0);
        assertThat(finder.getLo()).isNull();
    }

    /**
     * Fans each publish out to every subscription and holds a constant total delivery backlog, so the
     * per-subscription lag is small but the aggregate lag is {@code subscriptions} times larger.
     */
    private static final class FakeFanOutStandingBacklogSystem {
        private final int subscriptions;
        private final long standingDeliveryBacklog;
        long totalPublished;
        long totalReceived;

        FakeFanOutStandingBacklogSystem(int subscriptions, long standingDeliveryBacklog) {
            this.subscriptions = subscriptions;
            this.standingDeliveryBacklog = standingDeliveryBacklog;
        }

        void advance(double rate, long periodNanos) {
            totalPublished += (long) (rate * (periodNanos / 1e9));
            totalReceived = Math.max(0, subscriptions * totalPublished - standingDeliveryBacklog);
        }
    }

    /**
     * A broker that fans each publish out to every subscription: the consumers must move {@code
     * subscriptions * publishRate} deliveries to keep up, and {@code consumerCapacity} is the
     * aggregate delivery rate they can manage across all of them. The sustainable publish rate is
     * therefore {@code consumerCapacity / subscriptions}, independent of the producer's own ceiling.
     */
    private static final class FakeFanOutSystem {
        private final double producerCapacity;
        private final double consumerCapacity;
        private final int subscriptions;
        long totalPublished;
        long totalReceived;

        FakeFanOutSystem(double producerCapacity, double consumerCapacity, int subscriptions) {
            this.producerCapacity = producerCapacity;
            this.consumerCapacity = consumerCapacity;
            this.subscriptions = subscriptions;
        }

        void advance(double rate, long periodNanos) {
            double periodSeconds = periodNanos / 1e9;
            totalPublished += (long) (Math.min(rate, producerCapacity) * periodSeconds);
            long pendingDeliveries = subscriptions * totalPublished - totalReceived;
            totalReceived += (long) Math.min(consumerCapacity * periodSeconds, pendingDeliveries);
        }
    }
}
