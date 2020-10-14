/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.test.subscriber.AssertSubscriber;

public class MonoDeferComposeTest {

    @Test
    public void perTrackable() {

        Mono<Integer> source = Mono.just(10).transformDeferred(f -> {
            AtomicInteger value = new AtomicInteger();
            return f.map(v -> v + value.incrementAndGet());
        });


        for (int i = 0; i < 10; i++) {
            AssertSubscriber<Integer> ts = AssertSubscriber.create();

            source.subscribe(ts);

            ts.assertValues(11)
            .assertComplete()
            .assertNoError();
        }
    }

    @Test
    public void composerThrows() {
        Mono<Integer> source = Mono.just(10).transformDeferred(f -> {
            throw new RuntimeException("Forced failure");
        });

        for (int i = 0; i < 10; i++) {
            AssertSubscriber<Integer> ts = AssertSubscriber.create();

            source.subscribe(ts);

            ts.assertNoValues()
            .assertNotComplete()
            .assertError(RuntimeException.class);
        }

    }

    @Test
    public void composerReturnsNull() {
        Mono<Integer> source = Mono.just(10).transformDeferred(f -> {
            return null;
        });

        for (int i = 0; i < 10; i++) {
            AssertSubscriber<Integer> ts = AssertSubscriber.create();

            source.subscribe(ts);

            ts.assertNoValues()
            .assertNotComplete()
            .assertError(NullPointerException.class);
        }

    }
}
