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

package reactor.core.publisher;

import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class TailCalLSubscribeTest {

    private static Function<Flux<Object>, Flux<Object>> manyOperatorsOnFlux = flux -> {
        for (int i = 0; i < 5; i++) {
            flux = flux.<Object>map(Object::toString).filter(Objects::nonNull);
        }
        return flux;
    };

    private static Function<Mono<?>, Mono<Object>> manyOperatorsOnMono = mono -> {
        for (int i = 0; i < 5; i++) {
            mono = mono.<Object>map(Object::toString).filter(Objects::nonNull);
        }
        //noinspection unchecked
        return (Mono) mono;
    };

    @After
    public void tearDown() {
        Hooks.resetOnOperatorDebug();
    }

    @Test
    public void testStackDepth() throws Exception {
        StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();
        Mono
                .from(stackCapturingPublisher)
                .as(manyOperatorsOnMono)
                .flux()
                .as(manyOperatorsOnFlux)
                .delayElements(Duration.ofSeconds(1))
                .then()
                .subscribe(new CancellingSubscriber());

        assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
                .extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
                .startsWith(
                        tuple(Thread.class.getName(), "getStackTrace"),
                        tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
                        tuple(Mono.class.getName(), "subscribe"),
                        tuple(this.getClass().getName(), "testStackDepth")
                );
    }

    @Test
    public void testDebugHook() throws Exception {
        Hooks.onOperatorDebug();
        testStackDepth();
    }

    @Test
    public void interop() throws Exception {
        StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();
        Mono.from(stackCapturingPublisher)
                .as(manyOperatorsOnMono)
                .flux()
                .as(manyOperatorsOnFlux)
                .transform(flux -> flux::subscribe)
                .as(manyOperatorsOnFlux)
                .then()
                .as(manyOperatorsOnMono)
                .subscribe(new CancellingSubscriber());

        assertThat(stackCapturingPublisher.get(1, TimeUnit.SECONDS))
                .extracting(StackTraceElement::getClassName, StackTraceElement::getMethodName)
                .startsWith(
                        tuple(Thread.class.getName(), "getStackTrace"),
                        tuple(stackCapturingPublisher.getClass().getName(), "subscribe"),
                        tuple(Flux.class.getName(), "subscribe"),
                        tuple(Mono.class.getName(), "subscribe"),
                        tuple(this.getClass().getName(), "interop")
                );
    }

    private static class StackCapturingPublisher extends CompletableFuture<StackTraceElement[]> implements Publisher<Object> {

        @Override
        public void subscribe(Subscriber<? super Object> subscriber) {
            new Exception().printStackTrace(System.out);
            complete(Thread.currentThread().getStackTrace());
            subscriber.onSubscribe(Operators.emptySubscription());
            subscriber.onComplete();
        }
    }

    private static class CancellingSubscriber implements Subscriber<Object> {

        @Override
        public void onSubscribe(Subscription s) {
            s.cancel();
        }

        @Override
        public void onNext(Object s) {
        }

        @Override
        public void onError(Throwable throwable) {
            throwable.printStackTrace();
        }

        @Override
        public void onComplete() {
        }
    }
}