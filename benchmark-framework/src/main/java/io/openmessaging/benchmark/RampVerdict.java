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

/** How CHOP decides whether a held candidate rate is sustainable. See RATE_FINDING.md. */
public enum RampVerdict {
    /** Original: a candidate fails if publish/receive backlog exceeds a message-count limit. */
    BACKLOG,
    /** Achieved-rate + consumer-drain ratios (scale-free). */
    THROUGHPUT
}
