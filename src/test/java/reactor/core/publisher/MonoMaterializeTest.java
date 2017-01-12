/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class MonoMaterializeTest {

    @Test
    public void completeOnlyBackpressured() {
        AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);
        
        Mono.<Integer>empty().materialize()
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(Signal.complete())
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorOnlyBackpressured() {
        AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

        RuntimeException ex = new RuntimeException();
        
        Mono.<Integer>error(ex).materialize()
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(Signal.error(ex))
        .assertNoError()
        .assertComplete();
    }



    @Test
    public void materialize() {
        StepVerifier.create(Mono.just("Three")
                                .materialize())
                    .expectNextMatches(s -> s.isOnNext() && s.get()
                                                             .equals("Three"))
                    .verifyComplete();
    }


}
