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

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

import java.util.Queue;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public class SinkManyEmitterProcessorStressTest {

    @JCStressTest
    @Outcome(id = "0, 0", expect = ACCEPTABLE, desc = "Emission returned OK, but the concurrent drain cleaned the queue before the arbiter ran.")
    @Outcome(id = "3, 0", expect = ACCEPTABLE, desc = "Emission was correctly cancelled due to a race or because it happened post-cancellation.")
    @State
    public static class EmitNextAndAutoCancelRaceStressTest {

        private final SinkManyEmitterProcessor<Integer> sink = new SinkManyEmitterProcessor<>(true, 16);

        @Actor
        public void emitActor(II_Result r) {
            r.r1 = sink.tryEmitNext(1).ordinal();
        }

        @Actor
        public void cancelActor() {
            sink.asFlux().subscribe().dispose();
        }

        @Arbiter
        public void arbiter(II_Result r) {
            Queue<Integer> q = sink.queue;
            r.r2 = (q == null) ? 0 : q.size();
        }
    }
}