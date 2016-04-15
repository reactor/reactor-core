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

public class FluxDematerializerTest {

    Signal<Integer> error = Signal.<Integer>error(new RuntimeException("Forced failure"));
    
    @Test
    public void singleCompletion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Flux<Integer> dematerialize = Flux.just(error)
            .dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertNotComplete();
    }

    @Test
    public void immediateCompletion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(Signal.<Integer>complete()).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void immediateError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(error).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertNotComplete();
    }

    @Test
    public void completeAfterSingleSignal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.<Integer>complete()).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void errorAfterSingleSignal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(Signal.next(1), error).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertError(RuntimeException.class)
        .assertNotComplete();
    }

    @Test
    public void twoSignalsAndComplete() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), Signal.<Integer>complete()).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertNotComplete();

        ts.request(1);

        ts.assertValues(1, 2)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void twoSignalsAndError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), error).dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertNotComplete();

        ts.request(1);

        ts.assertValues(1, 2)
        .assertError(RuntimeException.class)
        .assertNotComplete();
    }

    @Test
    public void neverEnding() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Flux<Integer> dematerialize = Flux.just(Signal.next(1), Signal.next(2), 
                Signal.next(3), Signal.<Integer>complete())
        .concatWith(Flux.never())
        .dematerialize();
        
        dematerialize.subscribe(ts);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

}
