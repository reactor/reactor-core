package reactor.core.publisher;

import java.util.ArrayList;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxPrefetchTest {
//		1. EAGER / LAZY
//		3. request (limit - 1) && no doOnRequest && (limit - 1) elements
//		4. request 1 && (limit) doOnRequest && 1 elements
//		5. request (limit + 1) && (limit) doOnRequest && (limit + 1) elements
//		6. Complete (2 * limit + 1)

//		8. request 0 && error
//		7. Unbound

// SYNC-(NONE,ASYNC)-ASYNC (-BP,+P), ASYNC-(ASYNC,ASYNC)-ASYNC (-BP,-P), NONE-(NONE,ASYNC)-ASYNC (-BP,+P)
// SYNC-(SYNC,SYNC)-ANY    (-BP,-P), ASYNC-(ASYNC,ASYNC)-ANY   (-BP,-P), NONE-(NONE,ASYNC)-ANY   (-BP,+P)
// SYNC-(SYNC,NONE)-NONE   (+BP,-P), ASYNC-(ASYNC,NONE)-NONE   (+BP,-P), NONE-(NONE,NONE)-NONE   (+BP,+P)

//	TODO: DS - SYNC
//	TODO: DS - None
//	TODO: prefetchValuesFromAsyncFuseableUpstreamAndLazyRequestMode duplicate Eager
//	TODO: US - ASYNC -> no s.request

	@DisplayName("Prefetch value from Non-Fused upstream in Eager mode")
	@ParameterizedTest(name = "Prefetch value from Non-Fused upstream with downstream with fusion={0}")
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.ANY, Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.ASYNC | Fuseable.THREAD_BARRIER})
	public void prefetchValuesFromNonFuseableUpstreamInEagerRequestMode(int requestedMode) {
		int prefetch = 256;
		int limit = 192;

		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, limit * 2 + 1)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.create(nonFuseableSource.prefetch(prefetch,
				limit,
				FluxPrefetch.RequestMode.EAGER), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)

		            .thenAwait()
		            .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.NONE))
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(limit - 1)
		            .expectNextCount(limit - 1)
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(1)
		            .expectNextCount(1)
		            .then(() -> assertThat(requests).hasSize(2))

		            .thenRequest(limit + 1)
		            .expectNextCount(limit + 1)
		            .then(() -> assertThat(requests).hasSize(3))
		            .then(() -> assertThat(requests).containsExactly((long) prefetch,
				            (long) limit,
				            (long) limit))
		            .verifyComplete();
	}

	@DisplayName("Prefetch value from Non-Fused upstream in Lazy mode")
	@ParameterizedTest(name = "Prefetch value from Non-Fused upstream with downstream with fusion={0}")
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.ANY, Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.ASYNC | Fuseable.THREAD_BARRIER})
	public void prefetchValuesFromNonFuseableUpstreamInLazyRequestMode(int requestedMode) {
		int prefetch = 256;
		int limit = 192;

		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, limit * 2 + 1)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.create(nonFuseableSource.prefetch(prefetch, limit, FluxPrefetch.RequestMode.LAZY), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)

		            .thenAwait()
		            .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.NONE))
		            .then(() -> assertThat(requests).isEmpty())

		            .thenRequest(limit - 1)
		            .expectNextCount(limit - 1)
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(1)
		            .expectNextCount(1)
		            .then(() -> assertThat(requests).hasSize(2))

		            .thenRequest(limit + 1)
		            .expectNextCount(limit + 1)
		            .then(() -> assertThat(requests).hasSize(3))
		            .then(() -> assertThat(requests).containsExactly((long) prefetch,
				            (long) limit,
				            (long) limit))
		            .verifyComplete();
	}

	@Test
	public void prefetchValuesFromNonFuseableUpstreamInEagerRequestModeWithoutDownstreamFusion() {
		int prefetch = 256;
		int limit = 192;

		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, limit * 2 + 1)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.create(nonFuseableSource.prefetch(prefetch, limit, FluxPrefetch.RequestMode.EAGER), 0)
		            .expectSubscription()
		            .consumeSubscriptionWith(s -> {
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.NONE);
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).outputFused).isEqualTo(Fuseable.NONE);
		            })
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(limit - 1)
		            .expectNextCount(limit - 1)
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(1)
		            .expectNextCount(1)
		            .then(() -> assertThat(requests).hasSize(2))

		            .thenRequest(limit + 1)
		            .expectNextCount(limit + 1)
		            .then(() -> assertThat(requests).hasSize(3))
		            .then(() -> assertThat(requests).containsExactly((long) prefetch,
				            (long) limit,
				            (long) limit))
		            .verifyComplete();
	}

	@Test
	public void prefetchValuesFromNonFuseableUpstreamInLazyRequestModeWithoutDownstreamFusion() {
		int prefetch = 256;
		int limit = 192;

		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, limit * 2 + 1)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.create(nonFuseableSource.prefetch(prefetch, limit, FluxPrefetch.RequestMode.LAZY), 0)
		            .expectSubscription()
		            .consumeSubscriptionWith(s -> {
		            	assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.NONE);
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).outputFused).isEqualTo(Fuseable.NONE);
		            })
		            .then(() -> assertThat(requests).isEmpty())

		            .thenRequest(limit - 1)
		            .expectNextCount(limit - 1)
		            .then(() -> assertThat(requests).hasSize(1))

		            .thenRequest(1)
		            .expectNextCount(1)
		            .then(() -> assertThat(requests).hasSize(2))

		            .thenRequest(limit + 1)
		            .expectNextCount(limit + 1)
		            .then(() -> assertThat(requests).hasSize(3))
		            .then(() -> assertThat(requests).containsExactly((long) prefetch,
				            (long) limit,
				            (long) limit))
		            .verifyComplete();
	}

	@DisplayName("Prefetch Value from Async-Fused upstream in Eager mode")
	@ParameterizedTest(name = "Prefetch value from Async-Fused upstream with downstream with fusion={0}")
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.ANY, Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.ASYNC | Fuseable.THREAD_BARRIER})
	public void prefetchValuesFromAsyncFuseableUpstreamAndEagerRequestMode(int requestedMode) {
		int prefetch = 256;
		int count = 1000;

		Flux<Integer> asyncFuseableSource = Flux.range(1, count)
		                                      .onBackpressureBuffer();

		StepVerifier.create(asyncFuseableSource.prefetch(prefetch, FluxPrefetch.RequestMode.EAGER), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)

		            .thenAwait()
		            .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(requestedMode & Fuseable.ASYNC))

		            .thenRequest(500)
		            .expectNextCount(500)

		            .thenRequest(500)
		            .expectNextCount(500)
		            .verifyComplete();
	}

	@DisplayName("Prefetch Value from Async-Fused upstream in Lazy mode")
	@ParameterizedTest(name = "Prefetch value from Async-Fused upstream with downstream with fusion={0}")
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.ANY, Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.ASYNC | Fuseable.THREAD_BARRIER})
	public void prefetchValuesFromAsyncFuseableUpstreamAndLazyRequestMode(int requestedMode) {
		int prefetch = 256;
		int count = 1000;

		Flux<Integer> asyncFuseableSource = Flux.range(1, count)
		                                        .onBackpressureBuffer();

		StepVerifier.create(asyncFuseableSource.prefetch(prefetch, FluxPrefetch.RequestMode.LAZY), 0)
		            .expectFusion(requestedMode, requestedMode & Fuseable.ASYNC)

		            .thenAwait()
		            .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(requestedMode & Fuseable.ASYNC))

		            .thenRequest(500)
		            .expectNextCount(500)

		            .thenRequest(500)
		            .expectNextCount(500)
		            .verifyComplete();
	}

	@Test
	public void prefetchValuesFromAsyncFuseableUpstreamInEagerRequestModeWithoutDownstreamFusion() {
		int prefetch = 256;
		int count = 1000;

		Flux<Integer> asyncFuseableSource = Flux.range(1, count)
		                                        .onBackpressureBuffer();

		StepVerifier.create(asyncFuseableSource.prefetch(prefetch, FluxPrefetch.RequestMode.EAGER), 0)
		            .expectSubscription()
		            .consumeSubscriptionWith(s -> {
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.ASYNC);
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).outputFused).isEqualTo(Fuseable.NONE);
		            })

		            .thenRequest(500)
		            .expectNextCount(500)

		            .thenRequest(500)
		            .expectNextCount(500)
		            .verifyComplete();;
	}

	@Test
	public void prefetchValuesFromAsyncFuseableUpstreamInLazyRequestModeWithoutDownstreamFusion() {
		int prefetch = 256;
		int count = 1000;

		Flux<Integer> asyncFuseableSource = Flux.range(1, count)
		                                      .onBackpressureBuffer();

		StepVerifier.create(asyncFuseableSource.prefetch(prefetch, FluxPrefetch.RequestMode.LAZY), 0)
		            .expectSubscription()
		            .consumeSubscriptionWith(s -> {
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.ASYNC);
			            assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).outputFused).isEqualTo(Fuseable.NONE);
		            })

		            .thenRequest(500)
		            .expectNextCount(500)

		            .thenRequest(500)
		            .expectNextCount(500)
		            .verifyComplete();;
	}

	@DisplayName("Prefetch Value from Sync-Fused upstream in Eager mode")
	@ParameterizedTest(name = "Prefetch Value from Sync-Fused upstream with downstream with fusion={0}")
	@ValueSource(ints = {Fuseable.ASYNC, Fuseable.ANY, Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.ASYNC | Fuseable.THREAD_BARRIER})
	public void prefetchValuesFromSyncFuseableUpstreamAndEagerRequestMode(int requestedMode) {
		int prefetch = 256;
		int limit = 192;

		Flux<Integer> syncFuseableSource = Flux.range(1, limit * 2 + 1);

		StepVerifier.create(syncFuseableSource.prefetch(prefetch, limit, FluxPrefetch.RequestMode.EAGER), 0)
		            .expectFusion(requestedMode)
//		            .thenAwait(Duration.ofMillis(10))
//		            .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.SYNC & requestedMode))

                    .thenRequest(limit - 1)

//                    .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(Fuseable.SYNC & requestedMode))

                    .expectNextCount(limit - 1)

                    .thenRequest(1)
                    .expectNextCount(1)

                    .thenRequest(limit + 2)
                    .expectNextCount(limit + 1)

                    .verifyComplete();
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
}
