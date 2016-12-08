package reactor.test.publisher;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher.Violation;

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
	public void expectSubscribersN() {
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
		        .withMessageContaining("Expected minimum request of 6; got 5");

		publisher.assertCancelled();
		publisher.assertNoSubscribers();
		publisher.assertMinRequested(0);
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

}