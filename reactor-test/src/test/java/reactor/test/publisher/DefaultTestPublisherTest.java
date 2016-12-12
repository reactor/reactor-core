package reactor.test.publisher;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher.Violation;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DefaultTestPublisherTest {

	@Test(expected = NullPointerException.class)
	public void normalDisallowsNull() {
		TestPublisher<String> publisher = TestPublisher.create();

		publisher.next(null);
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

		try {
			StepVerifier.create(publisher, 1)
			            .then(() -> publisher.emit("foo", "bar"))
			            .expectNext("foo")
			            .expectComplete() //n/a
			            .verify();
			fail();
		}
		catch (AssertionError e) {
			assertThat(e.getMessage(), containsString("expected production of at most 1;"));
		}

		publisher.assertRequestOverflow();
	}

	@Test
	public void expectSubscribers() {
		TestPublisher<String> publisher = TestPublisher.create();

		try {
			publisher.assertSubscribers();
			fail("expected expectSubscribers to fail");
		} catch (AssertionError e) { }

		StepVerifier.create(publisher)
		            .then(() -> publisher.assertSubscribers()
		                                 .complete())
	                .expectComplete()
	                .verify();
	}

	@Test
	public void expectSubscribersN() {
		TestPublisher<String> publisher = TestPublisher.create();

		try {
			publisher.assertSubscribers(1);
			fail("expected expectSubscribers(1) to fail");
		} catch (AssertionError e) { }

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

		try {

		StepVerifier.create(Flux.from(publisher).limitRate(5))
		            .then(() -> publisher.assertMinRequested(6)
		                                 .emit("foo"))
		            .expectNext("foo").expectComplete() // N/A
		            .verify();
			fail("expected expectMinRequested(6) to fail");
		}
		catch (AssertionError e) {
			assertThat(e.getMessage(), containsString("Expected minimum request of 6; got 5"));
		}

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

		try {
			publisher.next(null, null); //this causes a compiler warning, on purpose
			fail("expected NPE");
		}
		catch (NullPointerException e) {
			assertThat(e.getMessage(), is("rest array is null, please cast to T if null T required"));
		}
	}

	@Test
	public void emitVarargNull() {
		TestPublisher<String> publisher = TestPublisher.create();

		try {
			publisher.emit(null); //this causes a compiler warning, on purpose
			fail("expected NPE");
		}
		catch (NullPointerException e) {
			assertThat(e.getMessage(), is("values array is null, please cast to T if null T required"));
		}
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