/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author Sergei Egorov
 */
@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class TailCallBenchmark {

    Flux<Object> source;

    @Param({"5", "10", "20", "50", "75", "100"})
    int operatorsCount;

    @Setup(Level.Trial)
    public void setup() {
        source = Flux.fromStream(() -> Stream.of(1, 2))
                .as(flux -> {
                    for (int i = 0; i < operatorsCount; i++) {
                        flux = flux.filter(it -> it % 2 == 0);
                    }
                    return flux;
                })
                .cast(Object.class);
    }

    @Benchmark
    public void without() {
        // See Flux#subscribe(Subscriber)
        CorePublisher<Object> publisher = Operators.onLastAssembly(source);
        CancelSubscriber<Object> cancelSubscriber = new CancelSubscriber<>();
        CoreSubscriber<Object> subscriber = Operators.<Object>toCoreSubscriber(cancelSubscriber);
        publisher.subscribe(subscriber);
        cancelSubscriber.join();
    }

    @Benchmark
    public void with() {
        CancelSubscriber<Object> subscriber = new CancelSubscriber<Object>();
        source.subscribe(subscriber);
        subscriber.join();
    }

    /**
     * Dummy subscriber that will cancel right after the subscription
     */
    static class CancelSubscriber<T> extends CompletableFuture<Void> implements Subscriber<T> {

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.cancel();
            complete(null);
        }

        @Override
        public void onNext(T o) {
        }

        @Override
        public void onError(Throwable throwable) {
            completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            complete(null);
        }
    }
}
