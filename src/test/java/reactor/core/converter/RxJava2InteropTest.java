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

package reactor.core.converter;

import java.util.NoSuchElementException;

import org.junit.Test;

import io.reactivex.*;
import reactor.core.flow.Fuseable;
import reactor.core.publisher.*;
import reactor.core.test.TestSubscriber;

public class RxJava2InteropTest {

    @Test
    public void flowableToFlux() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Flowable.range(1, 10)
        .hide()
        .to(RxJava2Interop::toFlux)
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flowableToFluxFused() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Flowable.range(1, 10)
        .to(RxJava2Interop::toFlux)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fluxToFlowable() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Flux.range(1, 10)
        .hide()
        .as(RxJava2Interop::toFlowable)
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fluxToFlowableFused() {
        io.reactivex.subscribers.TestSubscriber<Integer> ts = new io.reactivex.subscribers.TestSubscriber<>();
        ts.setInitialFusionMode(Fuseable.ANY);
        
        Flux.range(1, 10)
        .as(RxJava2Interop::toFlowable)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoErrors()
        .assertComplete();
    }
    
    @Test
    public void singleToMono() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Single.just(1)
        .to(RxJava2Interop::toMono)
        .subscribe(ts);
        
        ts
        .assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleToMonoFused() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Single.just(1)
        .to(RxJava2Interop::toMono)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.ASYNC)
        .assertValues(1)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void monoToSingle() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.just(1)
        .as(RxJava2Interop::toSingle)
        .subscribe(ts);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void monoEmptyToSingle() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        Mono.<Integer>empty()
        .as(RxJava2Interop::toSingle)
        .subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NoSuchElementException.class)
        .assertNotComplete();
    }

    @Test
    public void completableToMono() {
        TestSubscriber<Object> ts = TestSubscriber.create();
        
        Completable.complete()
        .to(RxJava2Interop::toMono)
        .subscribe(ts);
        
        ts
        .assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void completableToMonoFused() {
        TestSubscriber<Object> ts = TestSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Completable.complete()
        .to(RxJava2Interop::toMono)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.ASYNC)
        .assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void monoToCompletable() {
        TestSubscriber<Object> ts = TestSubscriber.create();
        
        Mono.empty()
        .as(RxJava2Interop::toCompletable)
        .subscribe(ts);
        
        ts
        .assertNoValues()
        .assertNoError()
        .assertComplete();
    }

}
