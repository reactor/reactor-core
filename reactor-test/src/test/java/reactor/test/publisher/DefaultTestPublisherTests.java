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
package reactor.test.publisher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher.Violation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DefaultTestPublisherTests {

	@Test
	public void normalDisallowsNull() {
		TestPublisher<String> publisher = TestPublisher.create();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.next(null))
				.withMessage("emitted values must be non-null");
	}

	@Test
	public void misbehavingAllowsNull() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.ALLOW_NULL);

		StepVerifier.create(publisher)
		            .then(() -> publisher.emit("foo", null))
		            .expectNext("foo", null)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalDisallowsOverflow() {
		TestPublisher<String> publisher = TestPublisher.create();

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
	public void misbehavingAllowsOverflow() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.REQUEST_OVERFLOW);

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(publisher, 1)
				                              .then(() -> publisher.emit("foo", "bar"))
				                              .expectNext("foo")
				                              .expectComplete() //n/a
				                              .verify())
				.withMessageContaining("expected production of at most 1;");

		publisher.assertRequestOverflow();
	}

	@Test
	public void normalIgnoresMultipleTerminations() {
		TestPublisher<String> publisher = TestPublisher.create();
		AtomicLong count = new AtomicLong();

		Subscriber<String> subscriber = new CoreSubscriber<String>() {
			@Override
			public void onSubscribe(Subscription s) { }

			@Override
			public void onNext(String s) { }

			@Override
			public void onError(Throwable t) {
				count.incrementAndGet();
			}

			@Override
			public void onComplete() {
				count.incrementAndGet();
			}
		};

		publisher.subscribe(subscriber);
		publisher.complete()
	             .emit("A", "B", "C")
	             .error(new IllegalStateException("boom"));

		assertThat(count.get()).isEqualTo(1);
	}

	private Subscriber<String> countingSubscriber(AtomicLong count) {
		return new CoreSubscriber<String>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
				s.cancel();
			}

			@Override
			public void onNext(String s) { }

			@Override
			public void onError(Throwable t) {
				count.incrementAndGet();
			}

			@Override
			public void onComplete() {
				count.incrementAndGet();
			}
		};
	}

	@Test
	public void misbehavingAllowsMultipleTerminations() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.CLEANUP_ON_TERMINATE);
		AtomicLong count = new AtomicLong();
		Subscriber<String> subscriber = countingSubscriber(count);

		publisher.subscribe(subscriber);

		publisher.error(new IllegalStateException("boom"))
		         .complete();

		publisher.emit("A", "B", "C");

		assertThat(count.get()).isEqualTo(3);
		publisher.assertCancelled();
	}

	@Test
	public void misbehavingMonoAllowsMultipleTerminations() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.CLEANUP_ON_TERMINATE);
		AtomicLong count = new AtomicLong();
		Subscriber<String> subscriber = countingSubscriber(count);

		publisher.mono().subscribe(subscriber);

		publisher.error(new IllegalStateException("boom"))
		         .complete();

		publisher.emit("A", "B", "C");

		assertThat(count.get()).isEqualTo(3);
		publisher.assertCancelled();
	}

	@Test
	public void misbehavingFluxAllowsMultipleTerminations() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.CLEANUP_ON_TERMINATE);
		AtomicLong count = new AtomicLong();

		Subscriber<String> subscriber = countingSubscriber(count);

		publisher.flux().subscribe(subscriber);

		publisher.error(new IllegalStateException("boom"))
		         .complete();

		publisher.emit("A", "B", "C");

		assertThat(count.get()).isEqualTo(3);
		publisher.assertCancelled();
	}

	@Test
	public void misbehavingFluxIgnoresCancellation() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.DEFER_CANCELLATION);
		AtomicLong emitCount = new AtomicLong();

		publisher.flux().subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				subscription.request(Long.MAX_VALUE);
				subscription.cancel();
			}

			@Override
			protected void hookOnNext(String value) {
				emitCount.incrementAndGet();
			}
		});

		publisher.emit("A", "B", "C");

		publisher.assertCancelled();
		assertThat(emitCount.get()).isEqualTo(3);
	}

	@Test
	public void misbehavingMonoCanAvoidCompleteAfterOnNext() {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(Violation.CLEANUP_ON_TERMINATE);
		AtomicLong terminalCount = new AtomicLong();

		publisher.mono()
		         .subscribe(countingSubscriber(terminalCount));

		assertThat(terminalCount).as("before onNext").hasValue(0L);

		publisher.next("foo");

		assertThat(terminalCount).as("after onNext").hasValue(0L);
	}

	@Test
	public void expectSubscribers() {
		TestPublisher<String> publisher = TestPublisher.create();

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
		TestPublisher<String> publisher = TestPublisher.create();

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
		TestPublisher<String> publisher = TestPublisher.create();

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
		TestPublisher<Object> publisher = TestPublisher.create();
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
		TestPublisher<String> publisher = TestPublisher.create();

		StepVerifier.create(Flux.from(publisher).limitRate(5))
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
		TestPublisher<String> publisher = TestPublisher.create();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> StepVerifier.create(Flux.from(publisher).limitRate(5))
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
		TestPublisher<String> publisher = TestPublisher.create();

		Flux.from(publisher).limitRequest(5).subscribe();
		publisher.assertMaxRequested(5);

		Flux.from(publisher).limitRequest(10).subscribe();
		publisher.assertSubscribers(2);
		publisher.assertMaxRequested(10);
	}

	@Test
	public void expectMaxRequestedWithUnbounded() {
		TestPublisher<String> publisher = TestPublisher.create();

		Flux.from(publisher).limitRequest(5).subscribe();
		publisher.assertMaxRequested(5);

		Flux.from(publisher).subscribe();
		publisher.assertSubscribers(2);
		publisher.assertMaxRequested(Long.MAX_VALUE);
	}

	@Test
	public void expectMaxRequestedFailure() {
		TestPublisher<String> publisher = TestPublisher.create();

		Flux.from(publisher).limitRequest(7).subscribe();

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> publisher.assertMaxRequested(6))
				.withMessage("Expected largest requested amount to be <= 6; got 7");
	}

	@Test
	public void emitCompletes() {
		TestPublisher<String> publisher = TestPublisher.create();
		StepVerifier.create(publisher)
	                .then(() -> publisher.emit("foo", "bar"))
	                .expectNextCount(2)
	                .expectComplete()
	                .verify();
	}

	@Test
	public void nextVarargNull() {
		TestPublisher<String> publisher = TestPublisher.create();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.next(null, null)) //this causes a compiler warning, on purpose
				.withMessage("rest array is null, please cast to T if null T required");
	}

	@Test
	public void emitVarargNull() {
		TestPublisher<String> publisher = TestPublisher.create();

		assertThatExceptionOfType(NullPointerException.class)
				.isThrownBy(() -> publisher.emit(null)) //this causes a compiler warning, on purpose
				.withMessage("values array is null, please cast to T if null T required");
	}

	@Test
	public void testError() {
		TestPublisher<String> publisher = TestPublisher.create();
		StepVerifier.create(publisher)
	                .then(() -> publisher.next("foo", "bar").error(new IllegalArgumentException("boom")))
	                .expectNextCount(2)
	                .expectErrorMessage("boom")
	                .verify();
	}



	@Test
	public void conditionalSupport() {
		TestPublisher<String> up = TestPublisher.create();
		StepVerifier.create(up.flux().filter("test"::equals), 2)
		            .then(() -> up.next("test"))
		            .then(() -> up.next("test2"))
		            .then(() -> up.emit("test"))
		            .expectNext("test", "test")
		            .verifyComplete();
	}

}
