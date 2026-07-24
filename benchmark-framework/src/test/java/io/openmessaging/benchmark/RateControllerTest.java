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

class RateControllerTest {
    private final RateController rateController = new RateController();
    private double rate = 10_000;
    private long periodNanos = SECONDS.toNanos(1);

    @Test
    void receiveBacklog() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 10_000);
        assertThat(rate).isEqualTo(20_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // receive backlog
        rate = rateController.nextRate(rate, periodNanos, 20_000, 15_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);
    }

    @Test
    void publishBacklog() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 10_000);
        assertThat(rate).isEqualTo(20_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // publish backlog
        rate = rateController.nextRate(rate, periodNanos, 15_000, 20_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);
    }

    @Test
    void heldDuringProducerStartupThenRampsUp() {
        // The finder starts at 10k msg/s, but the first control window(s) can be sampled while the
        // producer is still starting up (published ~ 0) — pronounced with the encrypt interceptor.
        // Treating that as saturation craters the rate to ~0, from which the multiplicative ramp
        // cannot recover (the observed encrypt-finder collapse to ~1 msg/s despite ~70k capacity).
        // The controller must HOLD the offered rate until the producer proves it can sustain it.
        rate = rateController.nextRate(rate, periodNanos, 0, 0);
        assertThat(rate).isEqualTo(10_000);
        // still basically nothing published — keep holding, do not crater
        rate = rateController.nextRate(rate, periodNanos, 100, 100);
        assertThat(rate).isEqualTo(10_000);
        // producer comes up and sustains the offered rate -> controller engages and ramps up
        rate = rateController.nextRate(rate, periodNanos, 10_100, 10_100);
        assertThat(rate).isEqualTo(20_000);
    }

    @Test
    void rampUp() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // receive backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 5_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 20_000, 20_000);
        assertThat(rate).isEqualTo(10_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(1);
    }
}
