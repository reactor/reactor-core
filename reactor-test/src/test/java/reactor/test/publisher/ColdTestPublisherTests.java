/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class ColdTestPublisherTests {

	@Test
	public void gh1236_test() {
		TestPublisher<String> result = TestPublisher.createCold();

		assertThat(result.emit("value").mono().block()).isEqualTo("value");
	}

	@Test
	public void coldWithSequentialSubscriptions() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.emit("A", "B", "C");

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C")
		            .verifyComplete();

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C")
		            .verifyComplete();

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C")
		            .verifyComplete();
	}

	@Test
	public void coldWithSequentialSubscriptionsAndTerminalSignalChanges() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.emit("A", "B", "C");

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C")
		            .verifyComplete();

		publisher.error(new IllegalStateException("boom"));

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C")
		            .verifyErrorMessage("boom");

		publisher.emit("D");

		StepVerifier.create(publisher.flux())
		            .expectNext("A", "B", "C", "D")
		            .verifyComplete();
	}

	@Test
	public void coldWithSignalEmittingWithinStepVerifier() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		StepVerifier.create(publisher)
		            .then(() -> publisher.emit("A", "B", "C"))
		            .expectNext("A", "B", "C")
		            .verifyComplete();
	}

	@Test
	public void normalDisallowsNull() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.next(null))
				.withMessage("emitted values must be non-null");
	}

	@Test
	public void misbehavingAllowsNull() {
		TestPublisher<String> publisher = TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.ALLOW_NULL);
		publisher.emit("foo", null);

		StepVerifier.create(publisher)
				.expectNext("foo", null)
				.expectComplete()
				.verify();
	}

	@Test
	public void normalDisallowsOverflow() {
		TestPublisher<String> publisher =
				TestPublisher.createColdNonBuffering();

		StepVerifier.create(publisher, 1)
		            .then(() -> publisher.next("foo")).as("should pass")
		            .then(() -> publisher.emit("bar")).as("should fail")
		            .expectNext("foo")
		            .expectErrorMatches(e -> e instanceof IllegalStateException &&
		                "Can't deliver value due to lack of requests".equals(e.getMessage()))
		            .verify();

		publisher.assertNoRequestOverflow();
	}

	@Test
	public void normalDisallowsOverflow2() {
		TestPublisher<String> publisher =
				TestPublisher.createColdNonBuffering();
		publisher.emit("foo", "bar");

		StepVerifier.create(publisher, 1)
				.expectNext("foo")
				.expectErrorMatches(e -> e instanceof IllegalStateException &&
						"Can't deliver value due to lack of requests".equals(e.getMessage()))
				.verify();

		publisher.assertNoRequestOverflow();
	}

	@Test
	public void misbehavingAllowsOverflow() {
		TestPublisher<String> publisher = TestPublisher.createColdNonCompliant(false, TestPublisher.Violation.REQUEST_OVERFLOW);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(publisher, 1)
						.then(() -> publisher.emit("foo", "bar"))
						.expectNext("foo")
						.verifyComplete())
				.withMessageContaining("expected production of at most 1;");

		publisher.assertRequestOverflow();
	}


	@Test
	public void coldAllowsMultipleReplayOnSubscribe() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.emit("A", "B");

		StepVerifier.create(publisher)
		            .expectNextCount(2).as("first")
		            .verifyComplete();

		StepVerifier.create(publisher)
		            .expectNextCount(2).as("second")
		            .verifyComplete();
	}

	@Test
	public void coldIgnoresMultipleTerminations() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		AtomicLong count = new AtomicLong();

		Subscriber<String> subscriber = new CoreSubscriber<String>() {
			@Override
			public void onSubscribe(Subscription s) { }

			@Override
			public void onNext(String s) { }

			@Override
			public void onError(Throwable t) {
				count.addAndGet(2L);
			}

			@Override
			public void onComplete() {
				count.addAndGet(1L);
			}
		};

		publisher.subscribe(subscriber);
		publisher.complete()
	             .emit("A", "B", "C")
	             .error(new IllegalStateException("boom"));

		assertThat(count).hasValue(1L);
	}

	@Test
	public void expectSubscribers() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(publisher::assertSubscribers)
				.withMessage("Expected subscribers");

		StepVerifier.create(publisher)
		            .then(() -> publisher.assertSubscribers()
		                                 .complete())
	                .expectComplete()
	                .verify();
	}

	@Test
	public void expectAssertSubscribersN() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> publisher.assertSubscribers(1))
		        .withMessage("Expected 1 subscribers, got 0");

		publisher.assertNoSubscribers();
		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(1);
		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(2);

		publisher.complete()
	             .assertNoSubscribers();
	}

	@Test
	public void expectSubscribersCountN() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> publisher.assertSubscribers(1))
				.withMessage("Expected 1 subscribers, got 0");

		assertThat(publisher.subscribeCount()).isEqualTo(0);
		publisher.assertNoSubscribers();

		Flux.from(publisher).subscribe();
		assertThat(publisher.subscribeCount()).isEqualTo(1);

		Flux.from(publisher).subscribe();
		assertThat(publisher.subscribeCount()).isEqualTo(2);

		publisher.complete().assertNoSubscribers();
		assertThat(publisher.subscribeCount()).isEqualTo(2);
	}

	@Test
	public void expectCancelled() {
		TestPublisher<Object> publisher = TestPublisher.createCold();
		StepVerifier.create(publisher)
	                .then(publisher::assertNotCancelled)
	                .thenCancel()
	                .verify();
		publisher.assertCancelled();

		StepVerifier.create(publisher)
	                .then(() -> publisher.assertCancelled(1))
	                .thenCancel()
	                .verify();
		publisher.assertCancelled(2);
	}

	@Test
	public void expectMinRequestedNormal() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		StepVerifier.create(Flux.from(publisher), 5)
	                .then(publisher::assertNotCancelled)
	                .then(() -> publisher.assertMinRequested(5))
	                .thenCancel()
	                .verify();
		publisher.assertCancelled();
		publisher.assertNoSubscribers();
		publisher.assertMinRequested(0);
	}

	@Test
	public void expectMinRequestedFailure() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.from(publisher), 5)
		            .then(() -> publisher.assertMinRequested(6)
		                                 .emit("foo"))
		            .expectNext("foo").expectComplete() // N/A
		            .verify())
		        .withMessageContaining("Expected smallest requested amount to be >= 6; got 5");

		publisher.assertCancelled();
		publisher.assertNoSubscribers();
		publisher.assertMinRequested(0);
	}

	@Test
	public void expectMaxRequestedNormal() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		Flux.from(publisher).limitRequest(5).subscribe();
		publisher.assertMaxRequested(5);

		Flux.from(publisher).limitRequest(10).subscribe();
		publisher.assertSubscribers(2);
		publisher.assertMaxRequested(10);
	}

	@Test
	public void expectMaxRequestedWithUnbounded() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		Flux.from(publisher).limitRequest(5).subscribe();
		publisher.assertMaxRequested(5);

		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(2);
		publisher.assertMaxRequested(Long.MAX_VALUE);
	}

	@Test
	public void expectMaxRequestedFailure() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		Flux.from(publisher).limitRequest(7).subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> publisher.assertMaxRequested(6))
				.withMessage("Expected largest requested amount to be <= 6; got 7");
	}

	@Test
	public void emitCompletes() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		StepVerifier.create(publisher)
	                .then(() -> publisher.emit("foo", "bar"))
	                .expectNextCount(2)
	                .expectComplete()
	                .verify();
	}

	@Test
	public void nextVarargNull() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.next(null, (String[]) null)) //end users forgetting to cast would see compiler warning as well
				.withMessage("rest array is null, please cast to T if null T required");
	}

	@Test
	public void emitVarargNull() {
		TestPublisher<String> publisher = TestPublisher.createCold();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.emit((String[])null)) //end users forgetting to cast would see compiler warning as well
				.withMessage("values array is null, please cast to T if null T required");
	}

	@Test
	public void testError() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		StepVerifier.create(publisher)
	                .then(() -> publisher.next("foo", "bar").error(new IllegalArgumentException("boom")))
	                .expectNextCount(2)
	                .expectErrorMessage("boom")
	                .verify();
	}

	@Test
	public void testErrorCold() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.next("foo", "bar").error(new IllegalArgumentException("boom"));
		StepVerifier.create(publisher)
	                .expectNextCount(2)
	                .expectErrorMessage("boom")
	                .verify();
	}

	@Test
	public void conditionalSupport() {
		TestPublisher<String> up = TestPublisher.createCold();
		StepVerifier.create(up.flux().filter("test"::equals), 2)
		            .then(() -> up.next("test"))
		            .then(() -> up.next("test2"))
		            .then(() -> up.emit("test"))
		            .expectNext("test", "test")
		            .verifyComplete();
	}

	@Test
	public void testBufferSupport() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.emit("A", "B", "C", "D", "E", "F");
		StepVerifier.create(publisher, 3L)
				.expectNext("A", "B", "C")
				.thenRequest(1L)
				.expectNext("D")
				.thenRequest(Long.MAX_VALUE)
				.expectNextCount(2L)
				.verifyComplete();
	}

	@Test
	public void testBufferSupportSubsequentDataAdded() {
		TestPublisher<String> publisher = TestPublisher.createCold();
		publisher.next("A", "B", "C", "D", "E", "F");
		StepVerifier.create(publisher, 3L)
				.expectNext("A", "B", "C")
				.thenRequest(1L)
				.expectNext("D")
				.thenRequest(Long.MAX_VALUE)
				.expectNextCount(2L)
				.then(() -> publisher.emit("G", "H"))
				.expectNext("G", "H")
				.verifyComplete();
	}
}
