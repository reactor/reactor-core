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
import reactor.core.publisher.FluxTailCallSubscribeTest.StackCapturingPublisher;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class MonoTailCallSubscribeTest {

    @After
    public void tearDown() {
        Hooks.resetOnOperatorDebug();
    }

    @Test
    public void testStackDepth() throws Exception {
        StackCapturingPublisher stackCapturingPublisher = new StackCapturingPublisher();
        Mono
                .from(stackCapturingPublisher)
                .map(Object::toString)
                .map(Object::toString)
                .map(Object::toString)
                .map(Object::toString)
                .map(Object::toString)
                .map(Object::toString)
                .filter(Objects::nonNull)
                .filter(Objects::nonNull)
                .filter(Objects::nonNull)
                .filter(Objects::nonNull)
                .filter(Objects::nonNull)
                .delayElement(Duration.ofSeconds(1))
                .subscribe(new Subscriber<String>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.cancel();
                    }

                    @Override
                    public void onNext(String s) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        throwable.printStackTrace();
                    }

                    @Override
                    public void onComplete() {

                    }
                });

        StackTraceElement[] stackTraceElements = stackCapturingPublisher.get(1, TimeUnit.SECONDS);

        assertThat(Arrays.asList(stackTraceElements).iterator())
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
}