/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;

public class FluxWindowTimeOrSizeTest {

	Flux<List<Integer>> scenario_windowWithTimeoutAccumulateOnTimeOrSize() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .windowTimeout(5, Duration.ofMillis(2000))
		           .concatMap(Flux::buffer);
	}

	@Test
	public void windowWithTimeoutAccumulateOnTimeOrSize() {
		StepVerifier.withVirtualTime(this::scenario_windowWithTimeoutAccumulateOnTimeOrSize)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	@Test
	public void longEmptyEmitsEmptyWindowsRegularly() {
		StepVerifier.withVirtualTime(() -> Mono.delay(Duration.ofMillis(350))
		                                       .ignoreElement()
		                                       .as(Flux::from)
		                                       .windowTimeout(1000, Duration.ofMillis(100))
		                                       .concatMap(Flux::collectList)
		)
		            .thenAwait(Duration.ofMinutes(1))
		            .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).isEmpty())
	                .verifyComplete();
	}

	@Test
	public void longDelaysStartEndEmitEmptyWindows() {
		StepVerifier.withVirtualTime(() ->
			Mono.just("foo")
			    .delayElement(Duration.ofMillis(400 + 400 + 300))
				.concatWith(Mono.delay(Duration.ofMillis(100 + 400 + 100)).then(Mono.empty()))
				.windowTimeout(1000, Duration.ofMillis(400))
				.concatMap(Flux::collectList)
		)
		            .thenAwait(Duration.ofHours(1))
	                .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).containsExactly("foo"))
	                .assertNext(l -> assertThat(l).isEmpty())
	                .assertNext(l -> assertThat(l).isEmpty()) //closing window
	                .verifyComplete();
	}

	@Test
	public void windowWithTimeoutStartsTimerOnSubscription() {
		StepVerifier.withVirtualTime(() ->
				Mono.delay(Duration.ofMillis(300))
				    .thenMany(Flux.range(1, 3))
				    .delayElements(Duration.ofMillis(150))
				    .concatWith(Flux.range(4, 10).delaySubscription(Duration.ofMillis(500)))
				    .windowTimeout(10, Duration.ofMillis(500))
				    .flatMap(Flux::collectList)
		)
		            .expectSubscription()
		            .thenAwait(Duration.ofSeconds(100))
		            .assertNext(l -> assertThat(l).containsExactly(1))
		            .assertNext(l -> assertThat(l).containsExactly(2, 3))
		            .assertNext(l -> assertThat(l).containsExactly(4, 5, 6, 7, 8, 9, 10, 11, 12, 13))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .verifyComplete();
	}

	@Test
	public void noDelayMultipleOfSize() {
		StepVerifier.create(Flux.range(1, 10)
		                        .windowTimeout(5, Duration.ofSeconds(1))
		                        .concatMap(Flux::collectList)
		)
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3, 4, 5))
		            .assertNext(l -> assertThat(l).containsExactly(6, 7, 8, 9, 10))
		            .assertNext(l -> assertThat(l).isEmpty())
		            .verifyComplete();
	}

	@Test
	public void noDelayGreaterThanSize() {
		StepVerifier.create(Flux.range(1, 12)
		                        .windowTimeout(5, Duration.ofHours(1))
		                        .concatMap(Flux::collectList)
		)
		            .assertNext(l -> assertThat(l).containsExactly(1, 2, 3, 4, 5))
		            .assertNext(l -> assertThat(l).containsExactly(6, 7, 8, 9, 10))
		            .assertNext(l -> assertThat(l).containsExactly(11, 12))
		            .verifyComplete();
	}

	@Test
	public void rejectedOnSubscription() {
		Scheduler testScheduler = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task) {
				return Scheduler.REJECTED;
			}

			@Override
			public Worker createWorker() {
				return new Worker() {
					@Override
					public Disposable schedule(Runnable task) {
						return Scheduler.REJECTED;
					}

					@Override
					public void dispose() {

					}
				};
			}
		};

		StepVerifier.create(Flux.range(1, 3).hide()
		                        .windowTimeout(10, Duration.ofMillis(500), testScheduler))
		            .verifyError(RejectedExecutionException.class);
	}

	@Test
	public void rejectedDuringLifecycle() {
		AtomicBoolean reject = new AtomicBoolean();
		Scheduler testScheduler = new Scheduler() {
			@Override
			public Disposable schedule(Runnable task) {
				return Scheduler.REJECTED;
			}

			@Override
			public Worker createWorker() {
				return new Worker() {

					Worker delegate = Schedulers.elastic().createWorker();

					@Override
					public Disposable schedule(Runnable task) {
						return REJECTED;
					}

					@Override
					public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
						if (reject.get())
							return Scheduler.REJECTED;
						return delegate.schedule(task, delay, unit);
					}

					@Override
					public void dispose() {
						delegate.dispose();
					}
				};
			}
		};

		StepVerifier.create(Flux.range(1, 3).hide()
		                        .windowTimeout(2, Duration.ofSeconds(2), testScheduler)
		                        .concatMap(w -> {
		                        	reject.set(true);
		                        	return w.collectList();
		                        })
		)
		            .assertNext(l -> assertThat(l).containsExactly(1, 2))
		            .verifyError(RejectedExecutionException.class);
	}

	@Test
    public void scanMainSubscriber() {
        Subscriber<Flux<Integer>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxWindowTimeOrSize.WindowTimeoutSubscriber<Integer> test = new FluxWindowTimeOrSize.WindowTimeoutSubscriber<>(actual,
        		123, 1000, Schedulers.single());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(123);
		test.requested = 35;
		Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
		Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(123);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onComplete();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}