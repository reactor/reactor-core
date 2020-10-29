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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

public class MonoPublishMulticastTest {

	@Test
	public void normal() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      .publish(o -> o.map(s -> 2));

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void normalHide() {
		AtomicInteger i = new AtomicInteger();
		Mono<Integer> m = Mono.fromCallable(i::incrementAndGet)
		                      .hide()
		                      .publish(o -> o.map(s -> 2));

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();

		StepVerifier.create(m)
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void cancelComposes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		TestPublisher<Integer> testPublisher = TestPublisher.create();

		testPublisher.mono()
		             .publish(o -> Mono.<Integer>never())
		             .subscribe(ts);

		testPublisher.assertNotCancelled()
		             .assertSubscribers();

		ts.cancel();

		testPublisher.assertNoSubscribers()
		             .assertCancelled();
	}

	@Test
	public void cancelComposes2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		TestPublisher<Integer> testPublisher = TestPublisher.create();

		testPublisher.mono()
		             .publish(o -> Mono.<Integer>empty())
		             .subscribe(ts);

		testPublisher.assertCancelled()
		             .assertNoSubscribers();
	}

	@Test
	public void nullFunction() {
		assertThatNullPointerException()
				.isThrownBy(() -> Mono.just("Foo")
				                      .publish(null))
				.withMessage("transform");
	}

	@Test
	public void npeFunction() {
		StepVerifier.create(Mono.just("Foo")
		                        .publish(m -> null))
		            .expectErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(NullPointerException.class)
				            .hasMessage("The transform returned a null Mono"))
		            .verify();
	}

	@Test
	public void failingFunction() {
		RuntimeException expected = new IllegalStateException("boom");
		StepVerifier.create(Mono.just("Foo")
		                        .publish(m -> {
			                        throw expected;
		                        }))
		            .expectErrorSatisfies(e -> assertThat(e).isSameAs(expected))
		            .verify();
	}

    @Test
    public void syncCancelBeforeComplete() {
        assertThat(Mono.just(Mono.just(1).publish(v -> v)).flatMapMany(v -> v).blockLast()).isEqualTo(1);
    }

    @Test
    public void normalCancelBeforeComplete() {
        assertThat(Mono.just(Mono.just(1).hide().publish(v -> v)).flatMapMany(v -> v).blockLast()).isEqualTo(1);
    }

	@Test
	public void scanMulticaster() {
		MonoPublishMulticast.MonoPublishMulticaster<Integer> test =
				new MonoPublishMulticast.MonoPublishMulticaster<>(Context.empty());
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(1);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
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
	public void scanMulticastInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		MonoPublishMulticast.MonoPublishMulticaster<Integer> parent =
				new MonoPublishMulticast.MonoPublishMulticaster<>(Context.empty());
		MonoPublishMulticast.PublishMulticastInner<Integer> test =
				new MonoPublishMulticast.PublishMulticastInner<>(parent, actual);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		test.request(789);
		//does not track request in the Mono version
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

}
