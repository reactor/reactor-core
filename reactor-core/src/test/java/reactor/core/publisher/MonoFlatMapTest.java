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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFlatMapTest {

	@Test
	public void normalHidden() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1).hide().flatMap(v -> Mono.just(2).hide()).subscribe(ts);

		ts.assertValues(2)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void cancel() {
		TestPublisher<String> cancelTester = TestPublisher.create();

		StepVerifier.create(cancelTester.mono()
										.flatMap(s -> Mono.just(s.length())))
					.thenCancel()
					.verify();

		cancelTester.assertCancelled();
	}

	@Test
	public void scanOperator(){
	    MonoFlatMap<String, Integer> test = new MonoFlatMap<>(Mono.just("foo"), s -> Mono.just(1));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanMain() {
		CoreSubscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMap.FlatMapMain<String, Integer> test = new MonoFlatMap.FlatMapMain<>(
				actual, s -> Mono.just(s.length()));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanInner() {
		CoreSubscriber<Integer> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoFlatMap.FlatMapMain<String, Integer> main = new MonoFlatMap.FlatMapMain<>(actual, s
				-> Mono.just(s.length()));
		MonoFlatMap.FlatMapInner<Integer> test = new MonoFlatMap.FlatMapInner<>(main);
		Subscription innerSubscription = Operators.emptySubscription();
		test.onSubscribe(innerSubscription);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(innerSubscription);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void noWrappingOfCheckedExceptions() {
		Mono.just("single")
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();
	}

	@Test
	public void noWrappingOfCheckedExceptions_hide() {
		Mono.just("single")
		    .hide()
		    .flatMap(x -> Mono.error(new NoSuchMethodException()))
		    .as(StepVerifier::create)
		    .expectError(NoSuchMethodException.class)
		    .verify();
	}
}
