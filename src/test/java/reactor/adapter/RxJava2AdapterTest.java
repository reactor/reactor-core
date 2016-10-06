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

package reactor.adapter;

import java.lang.reflect.Field;
import java.util.NoSuchElementException;

import org.junit.*;

import io.reactivex.*;
import io.reactivex.internal.fuseable.QueueSubscription;
import reactor.core.Fuseable;
import reactor.core.publisher.*;
import reactor.test.subscriber.AssertSubscriber;

public class RxJava2AdapterTest {
    @Test
    public void flowableToFlux() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        
        Flowable.range(1, 10)
        .hide()
        .to(RxJava2Adapter::flowableToFlux)
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void flowableToFluxFused() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Flowable.range(1, 10)
        .to(RxJava2Adapter::flowableToFlux)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.SYNC)
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fluxToFlowable() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        
        Flux.range(1, 10)
        .hide()
        .as(RxJava2Adapter::fluxToFlowable)
        .subscribe(ts);
        
        ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fluxToFlowableFused() throws Exception {
        io.reactivex.subscribers.TestSubscriber<Integer> ts = new io.reactivex.subscribers.TestSubscriber<>();
        
        // Testing for fusion is an RxJava 2 internal affair unfortunately
        Field f = io.reactivex.subscribers.TestSubscriber.class.getDeclaredField("initialFusionMode");
        f.setAccessible(true);
        f.setInt(ts, QueueSubscription.ANY);
        
        Flux.range(1, 10)
        .as(RxJava2Adapter::fluxToFlowable)
        .subscribe(ts);
        
        ts
        .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        .assertNoErrors()
        .assertComplete();
        
        // Testing for fusion is an RxJava 2 internal affair unfortunately
        f = io.reactivex.subscribers.TestSubscriber.class.getDeclaredField("establishedFusionMode");
        f.setAccessible(true);
        Assert.assertEquals(QueueSubscription.SYNC, f.getInt(ts));
    }
    
    @Test
    public void singleToMono() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        
        Single.just(1)
        .to(RxJava2Adapter::singleToMono)
        .subscribe(ts);
        
        ts
        .assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleToMonoFused() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Single.just(1)
        .to(RxJava2Adapter::singleToMono)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.ASYNC)
        .assertValues(1)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void monoToSingle() {
        Mono.just(1)
        .as(RxJava2Adapter::monoToSingle)
        .test()
        .assertValues(1)
        .assertNoErrors()
        .assertComplete();
    }

    @Test
    public void monoEmptyToSingle() {
        Mono.<Integer>empty()
        .as(RxJava2Adapter::monoToSingle)
        .test()
        .assertNoValues()
        .assertError(NoSuchElementException.class)
        .assertNotComplete();
    }

    @Test
    public void completableToMono() {
        AssertSubscriber<Object> ts = AssertSubscriber.create();
        
        Completable.complete()
        .to(RxJava2Adapter::completableToMono)
        .subscribe(ts);
        
        ts
        .assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void completableToMonoFused() {
        AssertSubscriber<Object> ts = AssertSubscriber.create();
        ts.requestedFusionMode(Fuseable.ANY);
        
        Completable.complete()
        .to(RxJava2Adapter::completableToMono)
        .subscribe(ts);
        
        ts
        .assertFusionMode(Fuseable.ASYNC)
        .assertNoValues()
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void monoToCompletable() {
        Mono.empty()
        .as(RxJava2Adapter::monoToCompletable)
        .test()
        .assertNoValues()
        .assertNoErrors()
        .assertComplete();
    }

}
