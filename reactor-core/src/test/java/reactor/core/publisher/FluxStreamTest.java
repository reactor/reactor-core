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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxStreamTest {

	final List<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

	@SuppressWarnings("ConstantConditions")
	@Test
	public void nullStream() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.fromStream((Stream<?>) null);
		});
	}

	@SuppressWarnings("ConstantConditions")
	@Test
	public void nullSupplier() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.fromStream((Supplier<Stream<?>>) null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredExact() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		Flux.fromStream(source.stream())
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iteratorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromStream(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10)
		                      .stream())
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void streamAlreadyConsumed() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Stream<Integer> s = source.stream();

		s.count();

		Flux.fromStream(s)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(IllegalStateException.class);
	}

	@Test
	public void streamConsumedBySubscription() {
		Stream<Integer> stream = source.stream();
		Flux<Integer> flux = Flux.fromStream(stream);

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();

		StepVerifier.create(flux)
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void streamGeneratedPerSubscription() {
		Flux<Integer> flux = Flux.fromStream(source::stream);

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();

		StepVerifier.create(flux)
		            .expectNextSequence(source)
		            .verifyComplete();
	}

	@Test
	public void nullSupplierErrorsAtSubscription() {
		Flux<String> flux = new FluxStream<>(() -> null);

		StepVerifier.create(flux)
		            .verifyErrorSatisfies(t -> assertThat(t).isInstanceOf(NullPointerException.class)
				            .hasMessage("The stream supplier returned a null Stream"));
	}

	@Test
	public void streamClosedOnCancelNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source))
		            .expectNext("foo")
		            .thenCancel()
		            .verify();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCancelSlowPathNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source), 1)
		            .expectNext("foo")
		            .thenCancel()
		            .verify();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCompletionNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source))
		            .expectNext("foo", "bar", "baz")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCompletionSlowPathNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source), 3)
		            .expectNext("foo", "bar", "baz")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnErrorNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .concatWith(Mono.error(new IllegalStateException("boom"))))
		            .expectNext("foo", "bar", "baz")
		            .verifyErrorMessage("boom");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnErrorSlowPathNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .concatWith(Mono.error(new IllegalStateException("boom"))),
				4)
		            .expectNext("foo", "bar", "baz")
		            .verifyErrorMessage("boom");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnNullContentNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", null, "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source))
		            .expectNext("foo", "bar")
		            .verifyErrorMessage("The iterator returned a null value");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnNullContentSlowPathNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", null, "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source), 4)
		            .expectNext("foo", "bar")
		            .verifyErrorMessage("The iterator returned a null value");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnPollCompletionNormal() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).map(Function.identity()))
		            .expectFusion()
		            .expectNext("foo", "bar")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamCloseFailureDroppedNormal() {
		Stream<String> source = Stream.of("foo", "bar")
		                              .onClose(() -> { throw new IllegalStateException("boom"); });

		StepVerifier.create(Flux.fromStream(source))
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void streamClosedOnCancelConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .filter(i -> true))
		            .expectNext("foo")
		            .thenCancel()
		            .verify();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCancelSlowPathConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).filter(i -> true), 0)
		            .thenRequest(1)
		            .expectNext("foo")
		            .thenCancel()
		            .verify();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCompletionConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).filter(i -> true))
		            .expectNext("foo", "bar", "baz")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnCompletionSlowPathConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).filter(i -> true), 3)
		            .expectNext("foo", "bar", "baz")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnErrorConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .filter(i -> true))
		            .expectNext("foo", "bar", "baz")
		            .verifyErrorMessage("boom");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnErrorSlowPathConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .concatWith(Mono.error(new IllegalStateException("boom")))
		                        .filter(i -> true), 4)
		            .expectNext("foo", "bar", "baz")
		            .verifyErrorMessage("boom");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnNullContentConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", null, "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).filter(i -> true))
		            .expectNext("foo", "bar")
		            .verifyErrorMessage("The iterator returned a null value");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnNullContentSlowPathConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar", null, "baz")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source).filter(i -> true), 4)
		            .expectNext("foo", "bar")
		            .verifyErrorMessage("The iterator returned a null value");

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamClosedOnPollCompletionConditional() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source)
		                        .filter(i -> true)
		                        .map(Function.identity()))
		            .expectFusion()
		            .expectNext("foo", "bar")
		            .verifyComplete();

		assertThat(closed).hasValue(1);
	}

	@Test
	public void streamCloseFailureDroppedConditional() {
		Stream<String> source = Stream.of("foo", "bar")
		                              .onClose(() -> { throw new IllegalStateException("boom"); });

		StepVerifier.create(Flux.fromStream(source).filter(i -> true))
		            .expectNext("foo", "bar")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void intermediateCloseIdempotent() {
		AtomicInteger closed = new AtomicInteger();
		Stream<String> source = Stream.of("foo", "bar")
		                              .onClose(closed::incrementAndGet);

		StepVerifier.create(Flux.fromStream(source), 1)
		            .expectNext("foo")
		            .then(source::close)
		            .then(() -> assertThat(closed).hasValue(1))
		            .thenRequest(1)
		            .expectNext("bar") //still working on the iterator
		            .verifyComplete();

		assertThat(closed).hasValue(1); //no double close
	}

	@Test
	public void scanOperator(){
		FluxStream<Integer> test = new FluxStream<>(() -> source.stream());

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
