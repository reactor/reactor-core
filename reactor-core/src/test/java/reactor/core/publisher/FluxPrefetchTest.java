package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxPrefetchTest {
//		1. EAGER / LAZY
//		3. request (limit - 1) && no doOnRequest && (limit - 1) elements
//		4. request 1 && (limit) doOnRequest && 1 elements
//		5. request (limit + 1) && (limit) doOnRequest && (limit + 1) elements
//		6. Complete (2 * limit + 1)

//		8. request 0 && error
//		7. Unbound

	@ParameterizedTest
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.SYNC, Fuseable.ANY})
	public void prefetchValuesFromNonFuseableUpstreamAndEagerRequestMode(int requestedMode) {
		int prefetch = 256;
		int limit = 192;

		ArrayList<Long> requests = new ArrayList<>();
		LongAdder requestCount = new LongAdder();
		AtomicLong requestLast = new AtomicLong();

		Flux<Integer> nonFuseableSource = Flux.range(1, 100_000_000)
		                                      .hide()
		                                      .doOnRequest(r -> requestCount.increment())
//		                                      .doOnRequest(requestLast::set)
		                                      .doOnRequest(requests::add);

		StepVerifier.create(nonFuseableSource.prefetch(prefetch,
				limit,
				FluxPrefetch.RequestMode.EAGER), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)
		            .then(() -> assertThat(requestCount.longValue()).isEqualTo(1))
		            .then(() -> assertThat(requestLast.get()).isEqualTo(prefetch))

//		            .then(() -> assertThat(requests).containsExactly((long) prefetch))

		            .thenRequest(limit - 1)
		            .expectNextCount(limit - 1)
		            .then(() -> assertThat(requestCount.longValue()).isEqualTo(1))
//		            .then(() -> assertThat(requestLast.get()).isEqualTo(prefetch))

//		            .then(() -> assertThat(requests).containsExactly((long) prefetch))

		            .thenRequest(1)
		            .expectNextCount(1)
		            .then(() -> assertThat(requestCount.longValue()).isEqualTo(2))
//		            .then(() -> assertThat(requestLast.get()).isEqualTo(limit))
//		            .then(() -> assertThat(requests).containsExactly((long) prefetch,
//				            (long) limit))

		            .thenRequest(limit + 1)
		            .expectNextCount(limit + 1)
		            .then(() -> assertThat(requestCount.longValue()).isEqualTo(3))
//		            .then(() -> assertThat(requestLast.get()).isEqualTo(limit))
		            .then(() -> assertThat(requests).containsExactly(prefetch, limit, limit))
		            .thenCancel()
		            .verify();
	}

	@ParameterizedTest
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.SYNC, Fuseable.ANY})
	public void downstreamFusionWithNonFuseableUpstreamAndEagerRequestMode(int requestedMode) {
		TestPublisher<Integer> publisher = TestPublisher.create();

		StepVerifier.create(publisher.flux()
		                             .prefetch(256, FluxPrefetch.RequestMode.EAGER), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)
		            .then(() -> publisher.assertMinRequested(256))
		            .then(publisher::complete)
		            .verifyComplete();
	}

	@ParameterizedTest
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.SYNC, Fuseable.ANY})
	public void downstreamFusionWithNonFuseableUpstreamAndLazyRequestMode(int requestMode) {
		TestPublisher<Integer> publisher = TestPublisher.create();

		StepVerifier.create(publisher.flux()
		                             .prefetch(256, FluxPrefetch.RequestMode.LAZY), 0)
		            .expectFusion(requestMode, requestMode & Fuseable.ASYNC)
		            .then(publisher::assertWasNotRequested)
		            .then(publisher::complete)
		            .verifyComplete();
	}
//
//	@Test
//	public void fusionWithSyncSource() {
//		AssertSubscriber<Object> subscriber = AssertSubscriber.create();
//		subscriber.requestedFusionMode(0);
//
//// SYNC-(SYNC)-SYNC, SYNC-(NONE,ASYNC)-ASYNC, SYNC-(SYNC)-ANY,
//// ASYNC-(NONE,SYNC)-SYNC, ASYNC-(ASYNC)-ASYNC, ASYNC-(ASYNC)-ANY
//// NONE-(NONE,NONE)-SYNC, NONE-(NONE,ASYNC)-ASYNC, NONE-(NONE,ASYNC)-ANY
//
//// SYNC-(SYNC,NONE)-NONE, ASYNC-(ASYNC,NONE)-NONE, NONE-(NONE)-NONE
//
//		Flux.range(1, 100_000_000)
//		    .prefetch()
//		    .subscribe(subscriber);
//
//		subscriber.assertNoValues()
//		          .assertNoError()
//		          .assertNotComplete()
//		          .assertFusionMode(1);
//
//	}
//
//	@Test
//	public void InsureOperatorPrefetchRequestValues() {
//		TestPublisher<Integer> publisher = TestPublisher.create();
//		AssertSubscriber<Object> subscriber = AssertSubscriber.create();
//
//		publisher.next(1)
//		         .complete();
//
//		publisher.req subscriber.requestedFusionMode()
//
//		StepVerifier.create(publisher.flux()
//		                             .prefetch(), 0)
//		            .expectFusion(Fuseable.SYNC, )
//		            .expectFusion()
//		            .thenRequest(1)
//		            .then(() -> {
//			            publisher.assertMaxRequested()
//		            })
//		            .expectComplete()
//		            .verify();
//
//		new FluxPrefetch<>(publisher.flux(), ); publisher.assertWasRequested();
//		publisher.assertMaxRequested(16);
//		publisher.assertMinRequested(12);
//
//		publisher
//	}
//
//	private static class TestSubscription implements Subscription {
//
//		@Override
//		public void request(long n) {
//			if (Operators.validate(n)) {
//				Operators.addCap(REQUESTED, this, n);
//				wasRequested = true;
//			}
//		}
//
//		@Override
//		public void cancel() {
//			this.isCancelled = true;
//		}
//	}
//
//	private static class TestQueueSubscription
//			implements Fuseable.QueueSubscription<Integer> {
//
//		@Override
//		public int requestFusion(int requestedMode) {
//			return 0;
//		}
//
//		@Override
//		@Nullable
//		public Integer poll() {
//			return null;
//		}
//
//		@Override
//		public int size() {
//			return 0;
//		}
//
//		@Override
//		public boolean isEmpty() {
//			return false;
//		}
//
//		@Override
//		public void clear() {
//
//		}
//
//		@Override
//		public void request(long n) {
//
//		}
//
//		@Override
//		public void cancel() {
//
//		}
//	}
}
