/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class MonoPublishMulticastTest {

	@Test
	void normal() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      .publish(o -> o.flatMap(s -> Mono.just(2)));

		StepVerifier.create(m)
		            .expectFusion()
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectFusion()
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	void normalHide() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      //actually, o isn't Fuseable to start with
		                      .publish(o -> o.map(s -> 2).hide());

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	void cancelComposes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> sp = MonoProcessor.create();

		sp.publish(o -> Mono.<Integer>never())
		  .subscribe(ts);

		assertThat(sp.downstreamCount() != 0).as("Not subscribed?").isTrue();

		ts.cancel();

		assertThat(sp.isCancelled()).as("Still subscribed?").isFalse();
	}

	@Test
	void cancelComposes2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		MonoProcessor<Integer> sp = MonoProcessor.create();

		sp.publish(o -> Mono.<Integer>empty())
		  .subscribe(ts);

		assertThat(sp.isCancelled()).as("Still subscribed?").isFalse();
	}

	@Test
	void nullFunction() {
		assertThatNullPointerException()
				.isThrownBy(() -> Mono.just("Foo")
				                      .publish(null))
				.withMessage("transform");
	}

	@Test
	void npeFunction() {
		StepVerifier.create(Mono.just("Foo")
		                        .publish(m -> null))
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The transform returned a null Mono"))
		            .verify();
	}

	@Test
	void failingFunction() {
		RuntimeException expected = new IllegalStateException("boom");
		StepVerifier.create(Mono.just("Foo")
		                        .publish(m -> {
			                        throw expected;
		                        }))
		            .expectErrorSatisfies(e -> assertThat(e).isSameAs(expected))
		            .verify();
	}

	@Test
	void syncCancelBeforeComplete() {
		assertThat(Mono.just(Mono.just(1).publish(v -> v)).flatMapMany(v -> v).blockLast()).isEqualTo(1);
	}

	@Test
	void normalCancelBeforeComplete() {
		assertThat(Mono.just(Mono.just(1).hide().publish(v -> v)).flatMapMany(v -> v).blockLast()).isEqualTo(1);
	}

	//see https://github.com/reactor/reactor-core/issues/2600
	@Test
	void errorFused() {
		final String errorMessage = "Error in Mono";
		final Mono<Object> source = Mono.error(new RuntimeException(errorMessage));
		final Mono<Object> published = source.publish(coordinator -> coordinator.flatMap(Mono::just));

		StepVerifier.create(published)
		            .expectFusion()
		            .expectErrorMessage(errorMessage)
		            .verify();

		StepVerifier.create(published, StepVerifierOptions.create().scenarioName("second shared invocation"))
		            .expectFusion()
		            .expectErrorMessage(errorMessage)
		            .verify();
	}

	//see https://github.com/reactor/reactor-core/issues/2600
	@Test
	void errorHide() {
		final String errorMessage = "Error in Mono";
		final Mono<Object> source = Mono.error(new RuntimeException(errorMessage));
		//value passed to Function is not Fuseable
		final Mono<Object> published = source.publish(Function.identity());

		StepVerifier.create(published)
		            .expectNoFusionSupport()
		            .expectErrorMessage(errorMessage)
		            .verify();

		StepVerifier.create(published, StepVerifierOptions.create().scenarioName("second shared invocation"))
		            .expectNoFusionSupport()
		            .expectErrorMessage(errorMessage)
		            .verify();
	}

	@Test
	void scanMulticaster() {
		MonoPublishMulticast.MonoPublishMulticaster<Integer> test =
				new MonoPublishMulticast.MonoPublishMulticaster<>(Context.empty());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
		test.value = 1;
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
		test.error = new IllegalArgumentException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.terminate();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void scanMulticastInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		MonoPublishMulticast.MonoPublishMulticaster<Integer> parent =
				new MonoPublishMulticast.MonoPublishMulticaster<>(Context.empty());
		MonoPublishMulticast.PublishMulticastInner<Integer> test =
				new MonoPublishMulticast.PublishMulticastInner<>(parent, actual);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		test.request(789);
		//does not track request in the Mono version
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
