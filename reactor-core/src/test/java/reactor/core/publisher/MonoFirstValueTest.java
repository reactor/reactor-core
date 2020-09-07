package reactor.core.publisher;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import java.time.Duration;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class MonoFirstValueTest {

	@Test
	public void firstSourceEmittingValueIsChosen() {
		StepVerifier.withVirtualTime(() -> Mono.firstValue(
				Mono.just(1).delayElement(Duration.ofMillis(500L)),
				Mono.just(2).delayElement(Duration.ofMillis(1_000L))
		))
				.thenAwait(Duration.ofMillis(1_500L))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void firstSourceEmittingValueIsChosenOverErrorOrCompleteEmpty() {
		StepVerifier.withVirtualTime(() -> Mono.firstValue(
				Mono.just(1).delayElement(Duration.ofMillis(500L)),
				Mono.error(new RuntimeException("Boom!")),
				Mono.empty(),
				Mono.never()
		))
				.thenAwait(Duration.ofMillis(500L))
				.expectNext(1)
				.verifyComplete();
	}

	@Test
	public void onlyErrorOrCompleteEmptyEmitsError() {
		StepVerifier.withVirtualTime(() -> Mono.firstValue(
				Mono.error(new RuntimeException("Boom!")),
				Mono.empty()
		))
				.expectErrorSatisfies(e -> {
					assertThat(e).isInstanceOf(NoSuchElementException.class);
					assertThat(e.getMessage()).isEqualTo("All sources completed with error or without values");
					Throwable throwable = e.getSuppressed()[0];
					assertThat(throwable.getSuppressed()[0].getMessage()).isEqualTo("Boom!");
					assertThat(throwable.getSuppressed()[1].getMessage())
							.isEqualTo("source at index 1 completed empty");

				})
				.verify();
	}

	@Test
	public void arrayNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstValue((Mono<Integer>[]) null));
	}

	@Test
	public void iterableNull() {
		assertThrows(NullPointerException.class, () -> Mono.firstValue((Iterable<Mono<Integer>>) null));
	}

	@Test
	public void cancelIsPropagated() {
		TestPublisher<Integer> pub1 = TestPublisher.create();
		TestPublisher<Integer> pub2 = TestPublisher.create();

		StepVerifier.create(Mono.firstValue(Mono.from(pub1), Mono.from(pub2)))
				.thenRequest(1)
				.then(() -> {
					pub1.emit(1 ).complete();
					pub2.emit(2).complete();
				})
				.expectNext(1)
				.thenCancel()
				.verify(Duration.ofSeconds(1L));

		pub1.assertWasSubscribed();
		pub1.assertMaxRequested(1);
		pub1.assertCancelled();

		pub2.assertWasSubscribed();
		pub2.assertMaxRequested(1);
		pub2.assertWasCancelled();
	}

	@Test
	public void singleArrayNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValue((Mono<Object>) null)
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValue(Mono.never(), null, Mono.never())
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValue(Arrays.asList((Mono<Object>) null))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.firstValue(Arrays.asList(Mono.never(),
				null,
				Mono.never()))
				.subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(NullPointerException.class);
	}

	@Test
	public void scanOperator() {
		MonoFirstValue<Integer> test = new MonoFirstValue<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void pairWise() {
		Mono<Integer> f = Mono.firstValue(Mono.just(1), Mono.just(2))
				.orValue(Mono.just(3));

		Assert.assertTrue(f instanceof MonoFirst);
		MonoFirst<Integer> s = (MonoFirst<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 3);

		f.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

	@Test
	public void pairWiseIterable() {
		Mono<Integer> f = Mono.first(Arrays.asList(Mono.just(1), Mono.just(2)))
				.or(Mono.just(3));

		Assert.assertTrue(f instanceof MonoFirst);
		MonoFirst<Integer> s = (MonoFirst<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 2);

		f.subscribeWith(AssertSubscriber.create())
				.assertValues(1)
				.assertComplete();
	}

}