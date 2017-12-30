/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxFilterFuseableTest extends FluxOperatorTest<String, String> {

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterFuseable.FilterFuseableSubscriber<String> test = new FluxFilterFuseable.FilterFuseableSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanConditionalSubscriber() {
        @SuppressWarnings("unchecked")
        Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxFilterFuseable.FilterFuseableConditionalSubscriber<String> test = new FluxFilterFuseable.FilterFuseableConditionalSubscriber<>(actual, t -> true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void failureStrategyResumeSyncFused() {
        Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
        try {
            StepVerifier.create(Flux.range(0, 2)
                                    .filter(i -> 4 / i == 4))
                        .expectFusion(Fuseable.ANY, Fuseable.SYNC)
                        .expectNext(1)
                        .expectComplete()
                        .verifyThenAssertThat()
                        .hasDroppedExactly(0)
                        .hasDroppedErrorWithMessage("/ by zero");
        }
        finally {
            Hooks.resetOnNextError();
        }
    }

    @Test
    public void failureStrategyResumeConditionalSyncFused() {
        Hooks.onNextError(OnNextFailureStrategy.RESUME_DROP);
        try {
            StepVerifier.create(Flux.range(0, 2)
                                    .filter(i -> 4 / i == 4)
                                    .filter(i -> true))
                        .expectFusion(Fuseable.ANY, Fuseable.SYNC)
                        .expectNext(1)
                        .expectComplete()
                        .verifyThenAssertThat()
                        .hasDroppedExactly(0)
                        .hasDroppedErrorWithMessage("/ by zero");
        }
        finally {
            Hooks.resetOnNextError();
        }
    }
}
