package reactor.core.publisher;

import java.util.ArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxPrefetchTest {
	private static final int sourceSize = 1000;

	private static final Object[] nonFuseableSource   =
			new Object[]{Flux.range(1, sourceSize).hide(), Fuseable.NONE};
	private static final Object[] syncFuseableSource  =
			new Object[]{Flux.range(1, sourceSize), Fuseable.SYNC};
	private static final Object[] asyncFuseableSource =
			new Object[]{Flux.range(1, sourceSize).onBackpressureBuffer(),
					Fuseable.ASYNC};

	private static Stream<Integer> requestedModes() {
		return Stream.of(Fuseable.NONE, Fuseable.ASYNC, Fuseable.ANY);
	}

	private static Stream<Object[]> requestedModesWithPrefetchModes() {
		return requestedModes().flatMap(requestMode -> Stream.of(true, false)
		                                                     .map(prefetchMode -> new Object[]{
				                                                     requestMode,
				                                                     prefetchMode}));
	}

	private static Stream<Object[]> sources() {
		return Stream.of(nonFuseableSource, syncFuseableSource, asyncFuseableSource);
	}

	// Fusions Tests
	@DisplayName("Prefetch value from Non-Fused upstream")
	@ParameterizedTest(name = "Prefetch value from Non-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromNonFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide();

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.thenAwait()
		    .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
				    Fuseable.NONE))

		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@DisplayName("Prefetch Value from Async-Fused upstream")
	@ParameterizedTest(name = "Prefetch value from Async-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromAsyncFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> asyncFuseableSource = Flux.range(1, sourceSize)
		                                        .onBackpressureBuffer();

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(asyncFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.thenAwait()
		    .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
				    Fuseable.ASYNC))

		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@DisplayName("Prefetch Value from Sync-Fused upstream")
	@ParameterizedTest(name = "Prefetch Value from Sync-Fused upstream with downstream with fusion={0} and Prefetch Mode={1}")
	@MethodSource("requestedModesWithPrefetchModes")
	public void prefetchValuesFromSyncFuseableUpstream(int requestedMode,
			boolean prefetchMode) {
		Flux<Integer> syncFuseableSource = Flux.range(1, sourceSize);

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(syncFuseableSource.prefetch(prefetchMode));

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode,
					(requestedMode & Fuseable.SYNC) != 0 ? Fuseable.SYNC : Fuseable.ASYNC)

			                .thenAwait()
			                .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
					                (requestedMode & Fuseable.SYNC) != 0 ? Fuseable.SYNC :
							                Fuseable.NONE));
		}
		else {
			step = firstStep.expectSubscription()
			                .thenAwait()
			                .consumeSubscriptionWith(s -> assertThat(((FluxPrefetch.PrefetchSubscriber<?>) s).sourceMode).isEqualTo(
					                Fuseable.SYNC));
		}

		step.expectNextCount(sourceSize)
		    .verifyComplete();
	}

	// Backpressure Tests
	@DisplayName("Check backpressure from different sources")
	@ParameterizedTest(name = "Prefetch value from {0}")
	@MethodSource("sources")
	public void backpressureFromDifferentSourcesTypes(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		source.prefetch()
		      .subscribe(ts);

		ts.assertNoEvents();

		int step = 250;
		int count = 0;
		while (count < sourceSize) {
			ts.request(step);
			count += step;
			ts.assertValueCount(count);
		}

		ts.assertComplete();
	}

	// Prefetch mode Tests
	@ParameterizedTest
	@MethodSource("requestedModes")
	public void immediatePrefetchInEagerPrefetchMode(int requestedMode) {
		int prefetch = 256;
		ArrayList<Long> requests = new ArrayList<>();

		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide()
		                                      .doOnRequest(requests::add);

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(prefetch, false), 0);

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, requestedMode & Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.then(() -> assertThat(requests).hasSize(1)
		                                    .containsExactly((long) prefetch))
		    .thenRequest(Long.MAX_VALUE)
		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}

	@ParameterizedTest
	@MethodSource("requestedModes")
	public void delayedPrefetchInLazyPrefetchMode(int requestedMode) {
		LongAdder requestCount = new LongAdder();

		Flux<Integer> nonFuseableSource = Flux.range(1, sourceSize)
		                                      .hide()
		                                      .doOnRequest((r) -> requestCount.increment());

		StepVerifier.FirstStep<Integer> firstStep =
				StepVerifier.create(nonFuseableSource.prefetch(true), 0);

		StepVerifier.Step<Integer> step;
		if (requestedMode != Fuseable.NONE) {
			step = firstStep.expectFusion(requestedMode, requestedMode & Fuseable.ASYNC);
		}
		else {
			step = firstStep.expectSubscription();
		}

		step.then(() -> assertThat(requestCount.longValue()).isEqualTo(0))
		    .thenRequest(Long.MAX_VALUE)
		    .expectNextCount(sourceSize)
		    .verifyComplete();
	}
}
