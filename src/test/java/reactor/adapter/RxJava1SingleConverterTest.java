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

import java.util.NoSuchElementException;

import org.junit.Test;

import reactor.core.publisher.*;
import reactor.test.TestSubscriber;
import rx.Single;

public class RxJava1SingleConverterTest {

    @Test
    public void singleToPublisherNormal() {
        
        Single<Integer> s = Single.just(1);
        

        Mono<Integer> m = RxJava1Adapter.singleToMono(s);
        
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        m.subscribe(ts);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleToPublisherNormalHidden() {
        
        Single<Integer> s = Single.just(1).doOnSuccess(e -> { });
        
        Mono<Integer> m = RxJava1Adapter.singleToMono(s);
        
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        m.subscribe(ts);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void singleToPublisherNormalBackpressured() {
        
        Single<Integer> s = Single.just(1);
        
        Mono<Integer> m = RxJava1Adapter.singleToMono(s);
        
        TestSubscriber<Integer> ts = TestSubscriber.create(0);
        
        m.subscribe(ts);
        
        ts.assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        ts.request(1);
        ts.request(1);
        
        ts.assertValues(1)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void singleToPublisherNull() {
        
        Single<Integer> s = Single.just(null);
        
        Mono<Integer> m = RxJava1Adapter.singleToMono(s);
        
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        m.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

    @Test
    public void singleToPublisherError() {
        
        Single<Integer> s = Single.error(new RuntimeException("Forced failure"));
        
        Mono<Integer> m = RxJava1Adapter.singleToMono(s);
        
        TestSubscriber<Integer> ts = TestSubscriber.create();
        
        m.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertNotComplete();
    }

    @Test
    public void publisherToSingleNormal() {
        
        Mono<Integer> m = Mono.just(1);
        
        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);
        
        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();
        
        s.subscribe(ts);
        
        ts.assertValue(1);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test
    public void publisherToSingleNormalHidden() {
        
        Mono<Integer> m = Mono.just(1).hide();
        
        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);
        
        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();
        
        s.subscribe(ts);
        
        ts.assertValue(1);
        ts.assertNoErrors();
        ts.assertCompleted();
    }

    @Test
    public void publisherToSingleEmpty() {
        
        Mono<Integer> m = Mono.empty();
        
        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);
        
        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();
        
        s.subscribe(ts);
        
        ts.assertNoValues();
        ts.assertError(NoSuchElementException.class);
        ts.assertNotCompleted();
    }

    @Test
    public void publisherToSingleEmptyHidden() {
        
        Mono<Integer> m = Mono.<Integer>empty().hide();

        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);

        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();

        s.subscribe(ts);

        ts.assertNoValues();
        ts.assertError(NoSuchElementException.class);
        ts.assertNotCompleted();
    }

    @Test
    public void publisherToSingleMore() {
        
        Flux<Integer> m = Flux.just(1, 2);
        
        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);
        
        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();
        
        s.subscribe(ts);
        
        ts.assertNoValues();
        ts.assertError(IndexOutOfBoundsException.class);
        ts.assertNotCompleted();
    }

    @Test
    public void publisherToSingleError() {
        
        Mono<Integer> m = Mono.error(new RuntimeException("Forced failure"));
        
        Single<Integer> s = RxJava1Adapter.publisherToSingle(m);
        
        rx.observers.TestSubscriber<Integer> ts = new rx.observers.TestSubscriber<>();
        
        s.subscribe(ts);
        
        ts.assertNoValues();
        ts.assertError(RuntimeException.class);
        ts.assertNotCompleted();
    }

}
