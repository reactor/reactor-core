/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.*;

public class MonoSingleOptionalTest {

	@Nested
	class ConcreteClassConsistency {
		//tests Flux.singleOptional, and Mono.singleOptional API consistency over returned classes

		@Test
		void monoWithScalarEmpty() {
			Mono<Integer> source = Mono.empty();
			Mono<Optional<Integer>> singleOptional = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(singleOptional).as("singleOptional")
			                  .isInstanceOf(MonoJust.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void monoWithScalarError() {
			Mono<Integer> source = Mono.error(new IllegalStateException("test"));
			Mono<Optional<Integer>> singleOptional = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(singleOptional).as("singleOptional")
			                  .isInstanceOf(MonoError.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void monoWithScalarValue() {
			Mono<Integer> source = Mono.just(1);
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single")
			                  .isInstanceOf(MonoJust.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void monoWithCallable() {
			Mono<Integer> source = Mono.fromSupplier(() -> 1);
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source")
			                  .isInstanceOf(Callable.class)
			                  .isNotInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single").isInstanceOf(MonoSingleOptionalCallable.class);
		}

		@Test
		void monoWithNormal() {
			Mono<Integer> source = Mono.just(1).hide();
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isNotInstanceOf(Callable.class); //excludes ScalarCallable too
			assertThat(single).as("single").isInstanceOf(MonoSingleOptionalMono.class);
		}

		@Test
		void fluxWithScalarEmpty() {
			Flux<Integer> source = Flux.empty();
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single")
			                  .isInstanceOf(MonoJust.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void fluxWithScalarError() {
			Flux<Integer> source = Flux.error(new IllegalStateException("test"));
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single")
			                  .isInstanceOf(MonoError.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void fluxWithScalarValue() {
			Flux<Integer> source = Flux.just(1);
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single")
			                  .isInstanceOf(MonoJust.class)
			                  .isInstanceOf(Fuseable.ScalarCallable.class);
		}

		@Test
		void fluxWithCallable() {
			Flux<Integer> source = Mono.fromSupplier(() -> 1).flux();
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source")
			                  .isInstanceOf(Callable.class)
			                  .isNotInstanceOf(Fuseable.ScalarCallable.class);
			assertThat(single).as("single").isInstanceOf(MonoSingleOptionalCallable.class);
		}

		@Test
		void fluxWithNormal() {
			Flux<Integer> source = Flux.range(1, 10);
			Mono<Optional<Integer>> single = source.singleOptional();

			assertThat(source).as("source").isNotInstanceOf(Callable.class); //excludes ScalarCallable too
			assertThat(single).as("single").isInstanceOf(MonoSingleOptional.class);
		}

	}

	@Test
	void source1Null() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingleOptional<>(null);
		});
	}

	@Test
	public void normal() {

		AssertSubscriber<Optional<Integer>> ts = AssertSubscriber.create();

		Flux.just(1).singleOptional().subscribe(ts);

		ts.assertValues(Optional.of(1))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Optional<Integer>> ts = AssertSubscriber.create(0);

		Flux.just(1).singleOptional().subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Optional.of(1))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {

		AssertSubscriber<Optional<Integer>> ts = AssertSubscriber.create();

		Flux.<Integer>empty().singleOptional().subscribe(ts);

		ts.assertValues(Optional.empty())
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .singleOptional())
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .singleOptional())
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void multi() {

		AssertSubscriber<Optional<Integer>> ts = AssertSubscriber.create();

		Flux.range(1, 10).singleOptional().subscribe(ts);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void multiBackpressured() {

		AssertSubscriber<Optional<Integer>> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).singleOptional().subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void singleCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .singleOptional())
		            .expectNext(Optional.of(1))
		            .verifyComplete();
	}

	@Test
	public void singleJustHide() {
		StepVerifier.create(Flux.empty()
								.hide()
		                        .singleOptional())
					.expectNext(Optional.empty())
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
	    MonoSingleOptional<String> test = new MonoSingleOptional<>(Flux.just("foo"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<Optional<String>>
				actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoSingleOptional.SingleOptionalSubscriber<String> test = new MonoSingleOptional.SingleOptionalSubscriber<>(
				actual);
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

}
