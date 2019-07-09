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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRefCountTest {

	//see https://github.com/reactor/reactor-core/issues/1738
	@Test
	public void avoidUnexpectedDoubleCancel() {
		AtomicBoolean unexpectedCancellation = new AtomicBoolean();

		Flux<Integer> test = Flux.range(0, 100)
		                         .delayElements(Duration.ofMillis(2))
		                         .publish()
		                         .refCount()
		                         .onBackpressureBuffer(); //known to potentially cancel twice, but that's another issue

		test.subscribe(v -> {}, e -> unexpectedCancellation.set(true));

		StepVerifier.create(test.take(3))
		            .expectNextCount(3)
		            .verifyComplete();

		assertThat(unexpectedCancellation).as("unexpected cancellation").isFalse();
	}

	//see https://github.com/reactor/reactor-core/issues/1385
	@Test
	public void sizeOneCanRetry() {
		AtomicInteger subCount = new AtomicInteger();

		final Flux<Object> flux = Flux
				.generate(() -> 0, (i, sink) -> {
					if (i == 2)
						sink.error(new RuntimeException("boom on subscribe #" + subCount.get()));
					else {
						sink.next(i);
					}
					return i + 1;
				})
				//we need to test with an async boundary
				.subscribeOn(Schedulers.parallel())
				.doOnSubscribe(s -> subCount.incrementAndGet());

		StepVerifier.create(flux.publish()
		                        .refCount(1)
		                        .retry(1))
		            .expectNext(0, 1, 0, 1)
		            .expectErrorMessage("boom on subscribe #2")
		            .verify(Duration.ofSeconds(1));
	}

	//see https://github.com/reactor/reactor-core/issues/1385
	@Test
	public void sizeOneCanRepeat() {
		AtomicInteger subCount = new AtomicInteger();

		final Flux<Object> flux = Flux
				.generate(() -> 0, (i, sink) -> {
					if (i == 2)
						sink.complete();
					else {
						sink.next(i);
					}
					return i + 1;
				})
				//we need to test with an async boundary
				.subscribeOn(Schedulers.parallel())
				.doOnSubscribe(s -> subCount.incrementAndGet());

		StepVerifier.create(flux.publish()
		                        .refCount(1)
		                        .repeat(1))
		            .expectNext(0, 1, 0, 1)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		assertThat(subCount).hasValue(2);
	}

	//see https://github.com/reactor/reactor-core/issues/1260
	@Test
	public void raceSubscribeAndCancel() {
		final Flux<String> testFlux = Flux.<String>create(fluxSink -> fluxSink.next("Test").complete())
				.replay(1)
				.refCount(1);

		final AtomicInteger signalCount1 = new AtomicInteger();
		final AtomicInteger signalCount2 = new AtomicInteger();

		final Runnable subscriber1 = () -> {
			for (int i = 0; i < 100_000; i++) {
				testFlux.next().doOnNext(signal -> signalCount1.incrementAndGet())
				        .subscribe();
			}
		};
		final Runnable subscriber2 = () -> {
			for (int i = 0; i < 100_000; i++) {
				testFlux.next().doOnNext(signal -> signalCount2.incrementAndGet())
				        .subscribe();
			}
		};

		RaceTestUtils.race(subscriber1, subscriber2);
		assertThat(signalCount1).as("signalCount1").hasValue(100_000);
		assertThat(signalCount2).as("signalCount2").hasValue(100_000);
	}

	@Test
	public void cancelDoesntTriggerDisconnectErrorOnFirstSubscribeNoComplete() {
		AtomicInteger nextCount = new AtomicInteger();
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		Flux<String> flux = Flux.<String>create(sink -> {
			sink.next("test");
		})
				.replay(1)
				.refCount(1);

		flux.subscribe(v -> nextCount.incrementAndGet(), errorRef::set);
		flux.next().subscribe(v -> nextCount.incrementAndGet(), errorRef::set);

		assertThat(nextCount).hasValue(2);
		assertThat(errorRef).hasValue(null);
	}

	@Test
	public void cancelDoesntTriggerDisconnectErrorOnFirstSubscribeDoComplete() {
		AtomicInteger nextCount = new AtomicInteger();
		AtomicReference<Throwable> errorRef = new AtomicReference<>();
		Flux<String> flux = Flux.<String>create(sink -> {
			sink.next("test");
			sink.complete();
		})
				.replay(1)
				.refCount(1);

		flux.subscribe(v -> nextCount.incrementAndGet(), errorRef::set);
		flux.next().subscribe(v -> nextCount.incrementAndGet(), errorRef::set);

		assertThat(nextCount).hasValue(2);
		assertThat(errorRef).hasValue(null);
	}

	@Test
	public void normal() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		Flux<Integer> p = e.publish().refCount();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(1);
		e.onNext(2);

		ts1.cancel();

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(3);

		ts2.cancel();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void normalTwoSubscribers() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();

		Flux<Integer> p = e.publish().refCount(2);

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(1);
		e.onNext(2);

		ts1.cancel();

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		e.onNext(3);

		ts2.cancel();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void upstreamCompletes() {

		Flux<Integer> p = Flux.range(1, 5).publish().refCount();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

	}

	@Test
	public void upstreamCompletesTwoSubscribers() {

		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}

	@Test
	public void subscribersComeAndGoBelowThreshold() {
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

		Disposable r = p.subscribe();
		r.dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();
		p.subscribe().dispose();

		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}

	@Test
	public void asyncRetry() {
		Flux<Integer> p;
		AtomicBoolean error = new AtomicBoolean();
		p = Flux.range(1, 5)
		        .subscribeOn(Schedulers.parallel())
		        .log("publish")
		        .publish()
		        .refCount(1)
		        .doOnNext(v -> {
			        if (v > 1 && error.compareAndSet(false, true)) {
				        throw new RuntimeException("test");
			        }
		        })
		        .log("retry")
		        .retry();

		StepVerifier.create(p)
		            .expectNext(1, 1, 2, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void reconnectsAfterRefCountZero() {
		AtomicLong subscriptionCount = new AtomicLong();
		AtomicReference<SignalType> termination = new AtomicReference<>();

		Flux<Integer> source = Flux.range(1, 50)
		                           .delayElements(Duration.ofMillis(100))
		                           .doFinally(termination::set)
		                           .doOnSubscribe(s -> subscriptionCount.incrementAndGet());

		Flux<Integer> refCounted = source.publish().refCount(2);

		Disposable sub1 = refCounted.subscribe();
		assertThat(subscriptionCount.get()).isZero();
		assertThat(termination.get()).isNull();

		Disposable sub2 = refCounted.subscribe();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub1.dispose();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isNull();

		sub2.dispose();
		assertThat(subscriptionCount.get()).isEqualTo(1);
		assertThat(termination.get()).isEqualTo(SignalType.CANCEL);

		try {
			sub1 = refCounted.subscribe();
			sub2 = refCounted.subscribe();
			assertThat(subscriptionCount.get()).isEqualTo(2);
		} finally {
			sub1.dispose();
			sub2.dispose();
		}
	}

	@Test
	public void delayElementShouldNotCancelTwice() throws Exception {
		DirectProcessor<Long> p = DirectProcessor.create();
		AtomicInteger cancellations = new AtomicInteger();

		Flux<Long> publishedFlux = p
			.publish()
			.refCount(2)
			.doOnCancel(() -> cancellations.incrementAndGet())
				.log();

		publishedFlux.any(x -> x > 5)
			.delayElement(Duration.ofMillis(2))
			.subscribe();

		CompletableFuture<List<Long>> result = publishedFlux.collectList().toFuture();

		for (long i = 0; i < 10; i++) {
			p.onNext(i);
			Thread.sleep(1);
		}
		p.onComplete();

		assertThat(result.get(10, TimeUnit.MILLISECONDS).size()).isEqualTo(10);
		assertThat(cancellations.get()).isEqualTo(1);
	}

	@Test
	public void scanMain() {
		ConnectableFlux<Integer> parent = Flux.just(10).publish();
		FluxRefCount<Integer> test = new FluxRefCount<>(parent, 17);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(256);
	}

	@Test
	public void scanInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxRefCount<Integer> main = new FluxRefCount<Integer>(Flux.just(10).publish(), 17);
		FluxRefCount.RefCountInner<Integer> test = new FluxRefCount.RefCountInner<Integer>(actual, new FluxRefCount.RefCountMonitor<>(main));
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isEqualTo(test.scan(Scannable.Attr.TERMINATED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after complete").isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after complete").isTrue();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after complete, cancel() ignored").isFalse();
	}

	@Test
	public void scanInnerCancelled() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxRefCount<Integer> main = new FluxRefCount<Integer>(Flux.just(10).publish(), 17);
		FluxRefCount.RefCountInner<Integer> test = new FluxRefCount.RefCountInner<Integer>(actual, new FluxRefCount.RefCountMonitor<>(main));
		Subscription sub = Operators.emptySubscription();
		test.onSubscribe(sub);
		test.cancel();

		assertThat(test.scan(Scannable.Attr.CANCELLED))
				.as("CANCELLED after cancel")
				.isNotEqualTo(test.scan(Scannable.Attr.TERMINATED))
				.isTrue();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).as("CANCELLED after cancel+onComplete").isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after cancel+onComplete").isFalse();
	}
}
