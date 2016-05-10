/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.Test;

import reactor.core.test.TestSubscriber;

public class FluxYieldTest {

    @Test
    public void yieldSome() {
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Flux<Integer> source = Flux.<Signal<Integer>>create(e -> {
            e.next(Signal.next(1));
            e.next(Signal.next(2));
            e.next(Signal.next(3));
            e.next(Signal.complete());
        }).dematerialize();
        
        source.subscribe(ts);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
}
