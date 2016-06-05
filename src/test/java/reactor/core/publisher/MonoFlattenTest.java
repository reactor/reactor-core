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

public class MonoFlattenTest {

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.just(1).hide().flatMap(v -> Flux.just(2).hide())
        .subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalInnerJust() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.just(1).hide().flatMap(v -> Flux.just(2))
        .subscribe(ts);
        
        ts.assertValues(2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalInnerEmpty() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.just(1).hide().flatMap(v -> Flux.<Integer>empty())
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
