/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxOnBackpressureBufferTimeout.BackpressureBufferTimeoutSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxOnBackpressureBufferTimeoutTest implements Consumer<Object> {

	final List<Object> evicted = Collections.synchronizedList(new ArrayList<>());

	@Override
	public void accept(Object t) {
		evicted.add(t);
	}

	@Test
	public void empty() {
		StepVerifier.create(Flux.empty().onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}))
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new IOException())
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}))
		            .verifyError(IOException.class);
	}

	@Test
	public void errorDelayed() {
		StepVerifier.create(Flux.just(1)
		                        .concatWith(Flux.error(new IOException()))
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}),
		0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(50))
		            .thenRequest(1)
		            .expectNext(1)
		            .verifyError(IOException.class);
	}

	@Test
	public void normal1() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal1SingleStep() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {})
		                        .limitRate(1))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal2() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}, Schedulers.single()))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal2SingleStep() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {}, Schedulers.single())
		                        .limitRate(1))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal3() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), 10, this, Schedulers.single()))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal3SingleStep() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), 10, this, Schedulers.single())
		                        .limitRate(1))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal4() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, this, Schedulers.single()))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void normal4SingleStep() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, this, Schedulers.single())
		                        .limitRate(1))
		            .expectNext(1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void bufferLimit() {
		StepVerifier.create(Flux.range(1, 5)
				.onBackpressureBuffer(Duration.ofMinutes(1), 1, this, Schedulers.single()),
				0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(100))
		            .thenRequest(1)
		            .expectNext(5)
		            .verifyComplete();

		assertThat(evicted).containsExactly(1, 2, 3, 4);
	}

	@Test
	public void timeoutLimit() {
		TestPublisher<Integer> tp = TestPublisher.create();
		StepVerifier.withVirtualTime(() ->
				tp.flux().onBackpressureBuffer(Duration.ofSeconds(1), 1, this, VirtualTimeScheduler.get()), 0)
		            .expectSubscription()
		            .then(() -> tp.next(1))
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> tp.next(2))
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> tp.next(3))
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> tp.next(4))
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(() -> tp.next(5))
		            .expectNoEvent(Duration.ofMillis(500))
		            .then(tp::complete)
		            .thenRequest(1)
		            .expectNext(5)
		            .verifyComplete();

		assertThat(evicted).containsExactly(1, 2, 3, 4);
	}

	@Test
	public void take() {
		StepVerifier.create(Flux.range(1, 5)
		                        .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, v -> {})
		                        .take(2))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void cancelEvictAll() {
		StepVerifier.create(Flux.range(1, 5)
		                        .log()
				.onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, this,
						Schedulers.single()),
				0)
		            .thenAwait(Duration.ofMillis(100)) //small hiccup to cancel after the prefetch
		            .thenCancel()
		            .verify();

		assertThat(evicted).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void timeoutEvictAll() {
		StepVerifier.withVirtualTime(() -> Flux.range(1, 5)
		                                       .onBackpressureBuffer(Duration.ofSeconds(1), Integer.MAX_VALUE, this, VirtualTimeScheduler.get()),
				0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenAwait(Duration.ofMinutes(1))
		            .thenRequest(1)
		            .verifyComplete();

		assertThat(evicted).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void evictCancels() {
		AtomicReference<Subscription> subscription = new AtomicReference<>();
		TestPublisher<Integer> tp = TestPublisher.create();
		StepVerifier.withVirtualTime(() -> tp.flux()
		                                     .doOnSubscribe(subscription::set)
		                                     .onBackpressureBuffer(Duration.ofSeconds(1), 10, i -> {
			                                     evicted.add(i);
			                                     subscription.get().cancel();
		                                     }, VirtualTimeScheduler.get()),
				0)
		            .then(() -> tp.emit(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMinutes(1))
		            .verifyComplete();

		tp.assertCancelled();
		assertThat(evicted).containsExactly(1, 2, 3, 4, 5);
	}

	@Test
	public void evictThrows() {

		TestPublisher<Integer> tp = TestPublisher.create();
		StepVerifier.withVirtualTime(() -> tp.flux()
		                                     .onBackpressureBuffer(Duration.ofSeconds(1), 10, i -> {
			                                     throw new IllegalStateException(i.toString());
		                                     }),
				0)
		            .then(() -> tp.emit(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMinutes(1))
		            .thenRequest(1)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedErrors(5)
		            .hasDroppedErrorsSatisfying(c -> {
			            Iterator<Throwable> it = c.iterator();
			            for (int i = 1; it.hasNext(); i++) {
			            	assertThat(it.next())
						            .isInstanceOf(IllegalStateException.class)
						            .hasMessage("" + i);
			            }
		            });
	}

	@Test
	public void cancelAndRequest() {
		List<Integer> seen = Collections.synchronizedList(new ArrayList<>());

		Flux.range(1, 5)
		    .onBackpressureBuffer(Duration.ofMinutes(1), Integer.MAX_VALUE, this, Schedulers.single())
		    .subscribe(new BaseSubscriber<Integer>() {
			    @Override
			    protected void hookOnSubscribe(Subscription subscription) {
				    request(1);
			    }

			    @Override
			    protected void hookOnNext(Integer value) {
			    	seen.add(value);
				    cancel();
			    }
		    });

		assertThat(seen).containsExactly(1);
	}

	@Test
	public void dropBySizeAndTimeout() {
		TestPublisher<Integer> tp = TestPublisher.create();

		StepVerifier.withVirtualTime(() -> tp.flux()
											// Note: using sub-millis durations after gh-1734
		                                     .onBackpressureBuffer(Duration.ofNanos(600),
				                                     5, this).log(),
				0)
		            .expectSubscription()
		            .then(() -> tp.next(1,2, 3, 4, 5, 6, 7)) //evict 2 elements
		            .then(() -> assertThat(evicted).containsExactly(1, 2))
		            .thenAwait(Duration.ofNanos(500))
		            .then(() -> tp.emit(8, 9, 10))
		            .thenAwait(Duration.ofNanos(100)) // evict elements older than 8
		            .then(() -> assertThat(evicted).containsExactly(1, 2, 3, 4, 5, 6, 7))
		            .thenRequest(10)
		            .expectNext(8, 9, 10)
		            .verifyComplete();
	}



	@Test
	public void gh1194() {
		StepVerifier.withVirtualTime(() ->
				Flux.just("1", "not requested", "not requested")
				    .onBackpressureBuffer(Duration.ofSeconds(1), 3, s -> {}), 1)
		            .expectNext("1")
		            .thenAwait(Duration.ofSeconds(1))
		            .verifyComplete();
	}

	@Test
	public void scanOperator() {
		Scannable test  = (Scannable) Flux.never().onBackpressureBuffer(Duration.ofSeconds(1), 123, v -> {}, Schedulers.single());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, null, null, null);
		BackpressureBufferTimeoutSubscriber<String> test = new BackpressureBufferTimeoutSubscriber<>(actual, Duration.ofSeconds(1), Schedulers.immediate(), 123, v -> {});
		Subscription s = Operators.emptySubscription();
		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(s);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(Long.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.offer("foo");
		test.offer("bar");
		assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(2);

		test.error = new RuntimeException("boom");
		assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isFalse();
		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}
}
