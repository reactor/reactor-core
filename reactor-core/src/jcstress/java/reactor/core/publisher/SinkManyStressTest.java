/*
 * Copyright (c) 2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.I_Result;

import java.util.Queue;

public class SinkManyStressTest {

    @JCStressTest
    @Outcome(id = {"0"}, expect = Expect.ACCEPTABLE, desc = "Queue was correctly cleared.")
    @Outcome(id = {"1"}, expect = Expect.FORBIDDEN, desc = "Item was leaked into the queue.")
    @State
    public static class SinkManyTakeZeroTest {
        final SinkManyEmitterProcessor<Object> sink = new SinkManyEmitterProcessor<>(true, 16);

        @Actor
        public void takeZero() {
            sink.asFlux().take(0).blockLast();
        }

        @Actor
        public void emit() {
            sink.tryEmitNext("Test emit");
        }

        @Arbiter
        public void arbiter(I_Result result) {
            Queue<?> q = sink.queue;
            result.r1 = q == null ? 0 : q.size();
        }

    }
}
