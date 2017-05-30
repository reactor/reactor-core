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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxOnBackpressureBufferTest
		extends FluxOperatorTest<String, String> {

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeHistory(){
		Flux.never().onBackpressureBuffer(-1);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(Flux::onBackpressureBuffer),

				scenario(f -> f.onBackpressureBuffer(4, d -> {}))
		);
	}

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ASYNC)
		                     .fusionModeThreadBarrier(Fuseable.ASYNC)
		                     .prefetch(Integer.MAX_VALUE);
	}

	@Test
	public void onBackpressureBuffer() {
		StepVerifier.create(Flux.range(1, 100)
		                        .onBackpressureBuffer(), 0)
		            .thenRequest(5)
		            .expectNext(1, 2, 3, 4, 5)
		            .thenAwait()
		            .thenRequest(90)
		            .expectNextCount(90)
		            .thenAwait()
		            .thenRequest(5)
		            .expectNext(96, 97, 98, 99, 100)
		            .verifyComplete();
	}

	@Test
	public void onBackpressureBufferMax() {
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8), 0)
		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .thenAwait()
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void onBackpressureBufferMaxCallback() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(16))
		            .thenRequest(9)
		            .expectNextCount(8)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void onBackpressureBufferMaxCallbackOverflow() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set, BufferOverflowStrategy.ERROR), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(16))
		            .thenRequest(9)
		            .expectNextCount(8)
		            .verifyErrorMatches(Exceptions::isOverflow);
	}

	@Test
	public void onBackpressureBufferMaxCallbackOverflow2() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set,
				                        BufferOverflowStrategy.DROP_OLDEST), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(92))
		            .thenRequest(9)
		            .expectNext(93, 94, 95, 96, 97, 98, 99, 100)
		            .verifyComplete();
	}

	@Test
	public void onBackpressureBufferMaxCallbackOverflow3() {
		AtomicInteger last = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .onBackpressureBuffer(8, last::set,
				                        BufferOverflowStrategy.DROP_LATEST), 0)

		            .thenRequest(7)
		            .expectNext(1, 2, 3, 4, 5, 6, 7)
		            .then(() -> assertThat(last.get()).isEqualTo(100))
		            .thenRequest(9)
		            .expectNext(8, 9, 10, 11, 12, 13, 14, 15)
		            .verifyComplete();
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureBuffer.BackpressureBufferSubscriber<Integer> test =
        		new FluxOnBackpressureBuffer.BackpressureBufferSubscriber<>(actual,
        				123, false, true, t -> {});
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.BooleanAttr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(0); // RS: TODO non-zero size

        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}