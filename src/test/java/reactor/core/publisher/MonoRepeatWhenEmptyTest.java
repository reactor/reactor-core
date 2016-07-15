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

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MonoRepeatWhenEmptyTest {

    @Test
    public void repeatInfinite() {
        AtomicInteger c = new AtomicInteger();

        Mono<String> source = Mono.defer(() -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just("test-data"));

        List<Long> iterations = new ArrayList<>();
        TestSubscriber<String> ts = TestSubscriber.create();

        source
            .repeatWhenEmpty(o -> o.doOnNext(iterations::add))
            .subscribe(ts);

        ts
            .assertValues("test-data")
            .assertComplete()
            .assertNoError();

        Assert.assertEquals(4, c.get());
        Assert.assertEquals(Arrays.asList(0L, 1L, 2L), iterations);
    }

    @Test
    public void repeatFinite() {
        AtomicInteger c = new AtomicInteger();

        Mono<String> source = Mono.defer(() -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just("test-data"));

        List<Long> iterations = new ArrayList<>();
        TestSubscriber<String> ts = TestSubscriber.create();

        source
            .repeatWhenEmpty(1000, o -> o.doOnNext(iterations::add))
            .subscribe(ts);

        ts
            .assertValues("test-data")
            .assertComplete()
            .assertNoError();

        Assert.assertEquals(4, c.get());
        Assert.assertEquals(Arrays.asList(0L, 1L, 2L), iterations);
    }

    @Test
    public void repeatFiniteExceeded() {
        AtomicInteger c = new AtomicInteger();

        Mono<String> source = Mono.defer(() -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just("test-data"));

        List<Long> iterations = new ArrayList<>();
        TestSubscriber<String> ts = TestSubscriber.create();

        source
            .repeatWhenEmpty(2, o -> o.doOnNext(iterations::add))
            .subscribe(ts);

        ts
            .assertError(IllegalStateException.class);

        Assert.assertEquals(3, c.get());
        Assert.assertEquals(Arrays.asList(0L, 1L), iterations);
    }

}
