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

import reactor.test.TestSubscriber;

public class FluxMapSignalTest {
    @Test
    public void completeOnlyBackpressured() {
        TestSubscriber<Integer> ts = TestSubscriber.create(0L);
        
        new FluxMapSignal<>(Flux.empty(), null, null, () -> 1)
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorOnlyBackpressured() {
        TestSubscriber<Integer> ts = TestSubscriber.create(0L);
        
        new FluxMapSignal<>(Flux.error(new RuntimeException()), null, e -> 1, null)
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

}
