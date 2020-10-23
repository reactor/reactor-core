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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

class MonoMaterializeTest {

	@Test
	void nextOnlyBackpressured() {
		AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

		Mono.just(1)
		    .materialize()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Signal.next(1))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	void completeOnlyBackpressured() {
		AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

		Mono.<Integer>empty()
				.materialize()
				.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Signal.complete())
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	void errorOnlyBackpressured() {
		AssertSubscriber<Signal<Integer>> ts = AssertSubscriber.create(0L);

		RuntimeException ex = new RuntimeException();

		Mono.<Integer>error(ex)
				.materialize()
				.subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(Signal.error(ex))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	void materialize() {
		StepVerifier.create(Mono.just("Three")
		                        .materialize())
		            .expectNextMatches(s -> s.isOnNext() && s.get().equals("Three"))
		            .verifyComplete();
	}

	@Test
	void valuedSourceMonoNotCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Mono<Integer> source = Mono.just(1)
		                                 .hide()
		                                 .doOnCancel(() -> cancelled.set(true));

		source.materialize()
		      .as(StepVerifier::create)
		      .expectNext(Signal.next(1))
		      .verifyComplete();

		assertThat(cancelled).isFalse();
	}

	@Test
	void emptySourceMonoNotCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Mono<?> source = Mono.empty()
		                           .hide()
		                           .doOnCancel(() -> cancelled.set(true));

		source.materialize()
		      .as(StepVerifier::create)
		      .expectNext(Signal.complete())
		      .expectComplete()
		      .verify(Duration.ofSeconds(5));

		assertThat(cancelled).isFalse();
	}

	@Test
	void errorSourceMonoNotCancelled() {
		AtomicBoolean cancelled = new AtomicBoolean();
		final Mono<?> source = Mono.error(new IllegalStateException("boom"))
		                           .hide()
		                           .doOnCancel(() -> cancelled.set(true));

		source.materialize()
		      .as(StepVerifier::create)
		      .assertNext(signal -> assertThat(signal.getThrowable()).isInstanceOf(IllegalStateException.class).hasMessage("boom"))
		      .verifyComplete();

		assertThat(cancelled).isFalse();
	}

	@Test
	void scanOperator() {
		MonoMaterialize<String> test = new MonoMaterialize<>(Mono.just("foo"));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
