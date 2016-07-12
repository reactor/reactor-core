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
package reactor.core.adapter;

import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.test.TestSubscriber;
import rx.Completable;
import rx.Observable;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Joao Pedro Evangelista
 */
public class RxJavaCompletableTests {

    private Completable completable;


    @Before
    public void setUp() throws Exception {
        completable = Completable.fromObservable(Observable.just(1,2,3,4,5));
    }

    @Test
    public void shouldConvertACompletableIntoMonoVoid() throws Exception {
        Mono<Void> printableMono = RxJava1Adapter.completableToMono(completable);
        assertThat(printableMono, is(notNullValue()));
        TestSubscriber
                .subscribe(printableMono)
                .assertNoValues()
                .assertComplete();
    }


    @Test
    public void completableWithErrorIntoMono() throws Exception {
        Mono<Void> voidMono = RxJava1Adapter.completableToMono(Completable.fromAction(() -> {
            throw new IllegalStateException("This should not happen"); //something internal happened
        }));
        TestSubscriber
                .subscribe(voidMono)
                .assertNoValues()
                .assertError(IllegalStateException.class)
                .assertTerminated()
                .assertNotComplete();
    }

    @Test
    public void shouldConvertAMonoIntoCompletable() throws Exception {
        Completable completable = RxJava1Adapter.publisherToCompletable(Mono.just(1));
        Throwable maybeErrors = completable.get();
        assertThat(maybeErrors, is(nullValue()));
    }
}
