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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxFilterWhenTest {

	@Test
	public void normal() {
		StepVerifier.withVirtualTime(() -> Flux.range(1, 10)
		                                       .filterWhen(v -> Mono.just(v % 2 == 0)
		                                                            .delayElement(Duration.ofMillis(100))))
		            .thenAwait(Duration.ofSeconds(5))
		            .expectNext(2, 4, 6, 8, 10)
		            .verifyComplete();
	}

	@Test
	public void normalSync() {
		StepVerifier.create(Flux.range(1, 10)
		                        .filterWhen(v -> Mono.just(v % 2 == 0).hide()))
		            .expectNext(2, 4, 6, 8, 10)
	                .verifyComplete();
	}

	@Test
	public void normalSyncFused() {
		StepVerifier.create(Flux.range(1, 10)
		        .filterWhen(v -> Mono.just(v % 2 == 0)))
		            .expectNext(2, 4, 6, 8, 10)
	                .verifyComplete();
	}

	@Test
	public void allEmpty() {
		StepVerifier.create(Flux.range(1, 10)
		                        .filterWhen(v -> Mono.<Boolean>empty().hide()))
		            .verifyComplete();
	}

	@Test
	public void allEmptyFused() {
		StepVerifier.create(Flux.range(1, 10)
		                        .filterWhen(v -> Mono.empty()))
		            .verifyComplete();
	}

	@Test
	public void empty() {
		StepVerifier.create(Flux.<Integer>empty()
								.filterWhen(v -> Mono.just(true)))
		            .verifyComplete();
	}

	@Test
	public void emptyBackpressured() {
		StepVerifier.create(Flux.<Integer>empty()
				.filterWhen(v -> Mono.just(true)), 0L)
				.verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.<Integer>error(new IllegalStateException())
				.filterWhen(v -> Mono.just(true)))
				.verifyError(IllegalStateException.class);
	}

	@Test
	public void errorBackpressured() {
		StepVerifier.create(Flux.<Integer>error(new IllegalStateException())
				.filterWhen(v -> Mono.just(true)), 0L)
				.verifyError(IllegalStateException.class);
	}

	@Test
	public void backpressureExactlyOne() {
		StepVerifier.create(Flux.just(1)
		                        .filterWhen(v -> Mono.just(true)), 1L)
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void longSourceSingleStep() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .filterWhen(v -> Flux.just(true).limitRate(1)))
		            .expectNextCount(1000)
		            .verifyComplete();
	}

	@Test
	public void longSource() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .filterWhen(v -> Mono.just(true).hide()))
		            .expectNextCount(1000)
		            .verifyComplete();
	}

	@Test
	public void longSourceFused() {
		StepVerifier.create(Flux.range(1, 1000)
		                        .filterWhen(v -> Mono.just(true)))
		            .expectNextCount(1000)
		            .verifyComplete();
	}

	@Test
	public void oneAndErrorInner() {
		StepVerifier.create(Flux.just(1)
		                        .filterWhen(v -> s -> {
			                        s.onSubscribe(Operators.emptySubscription());
			                        s.onNext(true);
			                        s.onError(new IllegalStateException());
		                        },16))
		            .expectNext(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrorsSatisfying(
				            c -> assertThat(c)
						            .hasSize(1)
						            .element(0).isInstanceOf(IllegalStateException.class)
		            );
	}

	@Test
	public void predicateThrows() {
		StepVerifier.create(Flux.just(1)
		                        .filterWhen(v -> { throw new IllegalStateException(); }))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void predicateNull() {
		StepVerifier.create(Flux.just(1).filterWhen(v -> null))
		            .verifyError(NullPointerException.class);
	}

	@Test
	public void predicateError() {
		StepVerifier.create(Flux.just(1)
		                        .filterWhen(v -> Mono.<Boolean>error(new IllegalStateException()).hide()))
		            .verifyError(IllegalStateException.class);
	}


	@Test
	public void predicateErrorFused() {
		StepVerifier.create(Flux.just(1)
		                        .filterWhen(v -> Mono.fromCallable(() -> { throw new IllegalStateException(); })))
		            .verifyError(IllegalStateException.class);
	}

	@Test
	public void take() {
		StepVerifier.create(Flux.range(1, 10)
		                        .filterWhen(v -> Mono.just(v % 2 == 0).hide())
		                        .take(1))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void take1Cancel() {
		AtomicLong onNextCount = new AtomicLong();
		AtomicReference<SignalType> endSignal = new AtomicReference<>();
		BaseSubscriber<Object> bs = new BaseSubscriber<Object>() {

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				requestUnbounded();
			}

			@Override
			public void hookOnNext(Object t) {
				onNextCount.incrementAndGet();
				cancel();
				onComplete();
			}

			@Override
			protected void hookFinally(SignalType type) {
				endSignal.set(type);
			}
		};

		Flux.range(1, 1000)
		    .filterWhen(v -> Mono.just(true).hide())
		    .subscribe(bs);

		assertThat(onNextCount).hasValue(1);
		assertThat(endSignal).hasValue(SignalType.CANCEL);
	}

	@Test
	public void take1CancelBackpressured() {
		AtomicLong onNextCount = new AtomicLong();
		AtomicReference<SignalType> endSignal = new AtomicReference<>();
		BaseSubscriber<Object> bs = new BaseSubscriber<Object>() {

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}

			@Override
			public void hookOnNext(Object t) {
				onNextCount.incrementAndGet();
				cancel();
				onComplete();
			}

			@Override
			protected void hookFinally(SignalType type) {
				endSignal.set(type);
			}
		};

		Flux.range(1, 1000)
		        .filterWhen(v -> Mono.just(true).hide())
		        .subscribe(bs);

		assertThat(onNextCount).hasValue(1);
		assertThat(endSignal).hasValue(SignalType.CANCEL);
	}


	@Test
	public void cancel() {
		TestPublisher<Boolean> assertCompanion = TestPublisher.create();

		StepVerifier.create(Flux.range(1, 5)
		                        .filterWhen(v -> assertCompanion.flux(), 16))
		            .thenCancel();

		assertCompanion.assertNoSubscribers();
	}

	@Test
	public void innerFluxCancelled() {
		AtomicInteger cancelCount = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 3)
		                        .filterWhen(v -> Flux.just(true, false, false)
		                                             .doOnCancel(cancelCount::incrementAndGet)))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(cancelCount).hasValue(3);
	}

	@Test
	public void innerFluxOnlyConsidersFirstValue() {
		StepVerifier.create(Flux.range(1, 3)
		                        .filterWhen(v -> Flux.just(false, true, true)))
		            .verifyComplete();
	}

	@Test
	public void innerMonoNotCancelled() {
		AtomicInteger cancelCount = new AtomicInteger();

		StepVerifier.create(Flux.range(1, 3)
		                        .filterWhen(v -> Mono.just(true)
		                                             .doOnCancel(cancelCount::incrementAndGet)))
		            .expectNext(1, 2, 3)
		            .verifyComplete();

		assertThat(cancelCount).hasValue(0);
	}

	@Test
	public void bufferSizeIsAlsoPrefetch() {
		AtomicLong requested = new AtomicLong();

		Flux.range(1, 10)
		    .hide()
		    .doOnRequest(r -> requested.compareAndSet(0, r))
		    .filterWhen(v -> Mono.just(v % 2 == 0), 5)
		    .subscribe().dispose();

		assertThat(requested).hasValue(5);
	}

	@Test
	public void largeBufferSize() {
		int bufferSize = 65536; //the buffer size given as example in javadoc
		AtomicLong requested = new AtomicLong();

		Flux.range(1, 10)
		    .hide()
		    .doOnRequest(r -> requested.compareAndSet(0, r))
		    .filterWhen(v -> Mono.just(v % 2 == 0), bufferSize)
		    .subscribe().dispose();

		assertThat(requested).hasValue(bufferSize);

		bufferSize = bufferSize + 1; //assert even if above it is still fine
		requested.set(0);
		Flux.range(1, 10)
		    .hide()
		    .doOnRequest(r -> requested.compareAndSet(0, r))
		    .filterWhen(v -> Mono.just(v % 2 == 0), bufferSize)
		    .subscribe().dispose();

		assertThat(requested).hasValue(bufferSize);
	}

	@Test
	public void introspectionNormal() {
		AtomicReference<Scannable> scannable = new AtomicReference<>();

		Flux<Integer> flux = Flux.range(1, 10)
		                         .filterWhen(i -> Mono.just(i % 2 == 0), 3)
		                         .doOnSubscribe(sub -> {
			                         assertThat(sub).isInstanceOf(Scannable.class);
			                         scannable.set((Scannable) sub);
		                         });

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .expectNext(2)
		            .then(() -> {
			            assertThat(scannable.get().scan(Scannable.Attr.PARENT)).isInstanceOf(FluxRange.RangeSubscription.class);
			            assertThat(scannable.get().scan(Scannable.Attr.ACTUAL)).isInstanceOf(FluxPeek.PeekSubscriber.class);
			            assertThat(scannable.get().scan(Scannable.Attr.PREFETCH)).isEqualTo(3);
			            assertThat(scannable.get().scan(Scannable.Attr.CAPACITY)).isEqualTo(4);
			            assertThat(scannable.get().scan(Scannable.Attr.ERROR)).isNull();
			            assertThat(scannable.get().scan(Scannable.Attr.BUFFERED )).isEqualTo(1);
			            assertThat(scannable.get().scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
			            assertThat(scannable.get().scan(Scannable.Attr.CANCELLED)).isEqualTo(false);
			            assertThat(scannable.get().scan(Scannable.Attr.TERMINATED)).isEqualTo(false);
		            })
	                .thenRequest(1)
	                .expectNext(4)
	                .then(() -> {
		                assertThat(scannable.get().scan(Scannable.Attr.ERROR)).isNull();
		                assertThat(scannable.get().scan(Scannable.Attr.BUFFERED )).isEqualTo(2);
		                assertThat(scannable.get().scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);
		                assertThat(scannable.get().scan(Scannable.Attr.CANCELLED)).isEqualTo(false);
		                assertThat(scannable.get().scan(Scannable.Attr.TERMINATED)).isEqualTo(false);
	                })
	                .thenRequest(3)
	                .expectNext(6, 8, 10)
	                .verifyComplete();

		assertThat(scannable.get().scan(Scannable.Attr.BUFFERED)).isEqualTo(6);
		assertThat(scannable.get().scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(5);
		assertThat(scannable.get().scan(Scannable.Attr.CANCELLED)).isEqualTo(false);
		assertThat(scannable.get().scan(Scannable.Attr.TERMINATED)).isEqualTo(true);
	}

	//TODO introspect errors (but is difficult due to Exceptions.terminate)
	@Test
	public void introspectionCancel() {
		AtomicReference<Scannable> scannable = new AtomicReference<>();

		Flux<Integer> flux = Flux.range(1, 10).concatWith(Mono.error(new IllegalStateException("boom")))
		                         .filterWhen(i -> Mono.just(i % 2 == 0), 3)
		                         .doOnSubscribe(sub -> {
		                         	assertThat(sub).isInstanceOf(Scannable.class);
			                         scannable.set((Scannable) sub);
		                         });

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .expectNext(2)
		            .then(() -> assertThat(scannable.get().scan(Scannable.Attr.CANCELLED)).isEqualTo(false))
		            .thenCancel()
		            .verify();

		assertThat(scannable.get().scan(Scannable.Attr.CANCELLED)).isEqualTo(true);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxFilterWhen<Integer> test = new FluxFilterWhen<>(parent, v -> Flux.just(true), 123);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterWhen.FluxFilterWhenSubscriber<String> test = new FluxFilterWhen.FluxFilterWhenSubscriber<>(actual, t -> Mono.just(true), 789);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
        assertThat(test.scan(Scannable.Attr.CAPACITY)).isEqualTo(1024); // next power of 2 of 789
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(789);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        test.error = new IllegalStateException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
    }

    @Test
    public void scanSmallBuffered() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterWhen.FluxFilterWhenSubscriber<String> test = new FluxFilterWhen.FluxFilterWhenSubscriber<>(actual, t -> Mono.just(true), 789);

        test.producerIndex = Integer.MAX_VALUE + 5L;
        test.consumerIndex = Integer.MAX_VALUE + 2L;
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(3);
        assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(3L);
    }

    @Test
    public void scanLargeBuffered() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterWhen.FluxFilterWhenSubscriber<String> test = new FluxFilterWhen.FluxFilterWhenSubscriber<>(actual, t -> Mono.just(true), 789);

        test.producerIndex = Integer.MAX_VALUE + 5L;
        test.consumerIndex = 2L;
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(Integer.MIN_VALUE);
        assertThat(test.scan(Scannable.Attr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE + 3L);
    }

    @Test
    public void scanInner() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFilterWhen.FluxFilterWhenSubscriber<String> main = new FluxFilterWhen.FluxFilterWhenSubscriber<>(actual, t -> Mono.just(true), 789);

        FluxFilterWhen.FilterWhenInner test = new FluxFilterWhen.FilterWhenInner(main, true);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(1L);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onNext(false);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
	public void filterAllOut() {
		final int[] calls = { 0 };

		StepVerifier.create(
				Flux.range(1, 1000)
				    .doOnNext(v -> calls[0]++)
				    .filterWhen(v -> Mono.just(false), 16)
				    .flatMap(ignore -> Flux.just(0)))
		            .verifyComplete();

		assertThat(calls[0]).isEqualTo(1000);
	}

	@Test
	public void filterAllOutHidden() {
		final int[] calls = { 0 };

		StepVerifier.create(
				Flux.range(1, 1000)
		        .doOnNext(v -> calls[0]++)
		        .filterWhen(v -> Mono.just(false).hide(), 16)
		        .flatMap(ignore -> Flux.just(0)))
		            .verifyComplete();

		assertThat(calls[0]).isEqualTo(1000);
	}
}
