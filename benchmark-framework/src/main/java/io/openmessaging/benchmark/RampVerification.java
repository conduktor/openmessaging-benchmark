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

/**
 * The window during which a CHOP ramp discovery held and confirmed its final accepted rate.
 * Attached to {@link TestResult} only when {@code rampAlgorithm == CHOP}, discovery ended in a
 * genuine confirm (not a safety-cap or rejected-confirmation outcome), and nothing during discovery
 * ever contradicted anything else -- so {@code nonMonotonic} is always {@code false} whenever this
 * object is actually present; a contradicted discovery is withheld entirely rather than reported as
 * a specific, possibly-unreproducible rate. The field is kept (rather than removed) for JSON schema
 * stability.
 */
public class RampVerification {
    public double rate;
    public long startEpochMillis;
    public long endEpochMillis;
    public boolean nonMonotonic;
}
