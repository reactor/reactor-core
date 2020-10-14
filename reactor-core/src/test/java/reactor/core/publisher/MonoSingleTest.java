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

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoSingleTest {
	@Test
	public void source1Null() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoSingle<>(null, 1, false);
		});
	}

	@Test
	public void defaultSupplierNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().single(null);
		});
	}


	@Test
	public void normal() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1).single().subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1).single().subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().single().subscribe(ts);

		ts.assertNoValues()
		  .assertError(NoSuchElementException.class)
		  .assertNotComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .single())
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .single())
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorDefault() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .single("bla"))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHideDefault() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .single("bla"))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void emptyDefault() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().single(1).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyDefaultBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().single(1).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void multi() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).single().subscribe(ts);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void multiBackpressured() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).single().subscribe(ts);

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
		                        .single())
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void singleFallbackEmpty() {
		StepVerifier.create(Flux.empty()
		                        .single(1))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void singleFallbackJust() {
		StepVerifier.create(Flux.just(1)
		                        .single(2))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void singleFallbackCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .single(2))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void singleJustHide() {
		StepVerifier.create(Flux.empty()
		                        .single())
		            .verifyError(NoSuchElementException.class);
	}

	@Test
	public void singleFallbackJustHide() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .single(2))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void singleEmptyFallbackCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .singleOrEmpty())
		            .expectNext(1)
		            .verifyComplete();
	}


	@Test
	public void singleEmptyFallbackJustHide() {
		StepVerifier.create(Flux.empty()
		                        .hide()
		                        .singleOrEmpty())
		            .verifyComplete();
	}

	@Test
	public void singleEmptyFallbackJustHideError() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .hide()
		                        .singleOrEmpty())
		            .verifyError(IndexOutOfBoundsException.class);
	}

	@Test
	public void scanOperator(){
	    MonoSingle<String> test = new MonoSingle<>(Flux.just("foo"));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String>
				actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoSingle.SingleSubscriber<String> test = new MonoSingle.SingleSubscriber<>(
				actual, "foo", false);
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
