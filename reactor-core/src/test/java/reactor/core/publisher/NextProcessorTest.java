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

import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.TestLogger;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class NextProcessorTest {

	@Test
	public void currentSubscriberCount() {
		Sinks.One<Integer> sink = new NextProcessor<>(null);

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asMono().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	@Test
	public void noRetentionOnTermination() throws InterruptedException {
		Date date = new Date();
		CompletableFuture<Date> future = new CompletableFuture<>();

		WeakReference<Date> refDate = new WeakReference<>(date);
		WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

		Mono<Date> source = Mono.fromFuture(future);
		Mono<String> data = source.map(Date::toString).as(NextProcessor::new);

		future.complete(date);
		assertThat(data.block()).isEqualTo(date.toString());

		date = null;
		future = null;
		source = null;
		System.gc();

		int cycles;
		for (cycles = 10; cycles > 0 ; cycles--) {
			if (refDate.get() == null && refFuture.get() == null) break;
			Thread.sleep(100);
		}

		assertThat(refFuture.get()).isNull();
		assertThat(refDate.get()).isNull();
		assertThat(cycles).isNotZero()
		                  .isPositive();
	}

	@Test
	public void noRetentionOnTerminationError() throws InterruptedException {
		CompletableFuture<Date> future = new CompletableFuture<>();

		WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

		Mono<Date> source = Mono.fromFuture(future);
		Mono<String> data = source.map(Date::toString).as(NextProcessor::new);

		future.completeExceptionally(new IllegalStateException());

		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(data::block);

		future = null;
		source = null;
		System.gc();

		int cycles;
		for (cycles = 10; cycles > 0; cycles--) {
			if (refFuture.get() == null) break;
			Thread.sleep(100);
		}

		assertThat(refFuture.get()).isNull();
		assertThat(cycles).isNotZero()
		                  .isPositive();
	}

	@Test
	public void noRetentionOnTerminationCancel() throws InterruptedException {
		CompletableFuture<Date> future = new CompletableFuture<>();

		WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

		Mono<Date> source = Mono.fromFuture(future);
		Mono<String> data = source.map(Date::toString).as(NextProcessor::new);

		future = null;
		source = null;

		data.subscribe().dispose();

		System.gc();

		int cycles;
		for (cycles = 10; cycles > 0 ; cycles--) {
			if (refFuture.get() == null) break;
			Thread.sleep(100);
		}

		assertThat(refFuture.get()).isNull();
		assertThat(cycles).isNotZero()
		                  .isPositive();
	}

	@Test
	public void MonoProcessorResultNotAvailable() {
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			NextProcessor<String> mp = new NextProcessor<>(null);
			mp.block(Duration.ofMillis(1));
		});
	}

	@Test
	public void rejectedDoOnSuccessOrError() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = mp.doOnSuccessOrError((s, f) -> ref.set(f));
		mono.subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void rejectedDoOnTerminate() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(invoked).hasValue(1);
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void rejectedSubscribeCallback() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void successDoOnSuccessOrError() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<String> ref = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = mp.doOnSuccessOrError((s, f) -> ref.set(s));
		mono.subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void successDoOnTerminate() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe();
		mp.onNext("test");

		assertThat(invoked).hasValue(1);
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void successSubscribeCallback() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<String> ref = new AtomicReference<>();

		mp.subscribe(ref::set);
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void rejectedDoOnError() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void rejectedSubscribeCallbackNull() {
		NextProcessor<String> mp = new NextProcessor<>(null);

		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			mp.subscribe((Subscriber<String>) null);
		});
	}

	@Test
	public void successDoOnSuccess() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccess(ref::set).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void successChainTogether() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		NextProcessor<String> mp2 = new NextProcessor<>(null);
		mp.subscribe(mp2);

		mp.onNext("test");

		assertThat(mp2.peek()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void rejectedChainTogether() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		NextProcessor<String> mp2 = new NextProcessor<>(null);
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void doubleFulfill() {
		NextProcessor<String> mp = new NextProcessor<>(null);

		StepVerifier.create(mp)
		            .then(() -> {
			            mp.onNext("test1");
			            mp.onNext("test2");
		            })
		            .expectNext("test1")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedExactly("test2");
	}

	@Test
	public void nullFulfill() {
		NextProcessor<String> mp = new NextProcessor<>(null);

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	public void mapFulfill() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);

		mp.onNext(1);

		NextProcessor<Integer> mp2 = mp.map(s -> s * 2)
		                               .cache()
		                               .subscribeWith(new NextProcessor<>(null));
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void thenFulfill() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);

		mp.onNext(1);

		NextProcessor<Integer> mp2 = mp.flatMap(s -> Mono.just(s * 2))
		                               .subscribeWith(new NextProcessor<>(null));
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void mapError() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);

		mp.onNext(1);

		NextProcessor<Integer> mp2 = new NextProcessor<>(null);

		StepVerifier.create(mp.<Integer>map(s -> {
			throw new RuntimeException("test");
		}).subscribeWith(mp2), 0)
		            .thenRequest(1)
		            .then(() -> {
			            assertThat(mp2.isTerminated()).isTrue();
			            assertThat(mp2.isSuccess()).isFalse();
			            assertThat(mp2.getError()).hasMessage("test");
		            })
		            .verifyErrorMessage("test");
	}

	@Test
	public void doubleError() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			NextProcessor<String> mp = new NextProcessor<>(null);

			mp.onError(new Exception("test"));
			mp.onError(new Exception("test2"));
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("test2");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void doubleSignal() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			NextProcessor<String> mp = new NextProcessor<>(null);

			mp.onNext("test");
			mp.onError(new Exception("test2"));

			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("test2");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void zipMonoProcessor() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		NextProcessor<Tuple2<Integer, Integer>> mp3 = new NextProcessor<>(null);

		StepVerifier.create(Mono.zip(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isTerminated()).isFalse())
		            .then(() -> mp.onNext(1))
		            .then(() -> assertThat(mp3.isTerminated()).isFalse())
		            .then(() -> mp2.onNext(2))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.peek()
			                          .getT1()).isEqualTo(1);
			            assertThat(mp3.peek()
			                          .getT2()).isEqualTo(2);
		            })
		            .expectNextMatches(t -> t.getT1() == 1 && t.getT2() == 2)
		            .verifyComplete();
	}

	@Test
	public void zipMonoProcessor2() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp3 = new NextProcessor<>(null);

		StepVerifier.create(Mono.zip(d -> (Integer)d[0], mp)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isTerminated()).isFalse())
		            .then(() -> mp.onNext(1))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.peek()).isEqualTo(1);
		            })
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void zipMonoProcessorRejected() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		NextProcessor<Tuple2<Integer, Integer>> mp3 = new NextProcessor<>(null);

		StepVerifier.create(Mono.zip(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isTerminated()).isFalse())
		            .then(() -> mp.onError(new Exception("test")))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isFalse();
			            assertThat(mp3.getError()).hasMessage("test");
		            })
		            .verifyErrorMessage("test");
	}


	@Test
	public void filterMonoProcessor() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isFalse())
		            .then(() -> assertThat(mp2.isSuccess()).isTrue())
		            .then(() -> assertThat(mp2.peek()).isEqualTo(2))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
	public void filterMonoProcessorNot() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		StepVerifier.create(mp.filter(s -> s % 2 == 0).subscribeWith(mp2))
		            .then(() -> mp.onNext(1))
		            .then(() -> assertThat(mp2.isError()).isFalse())
		            .then(() -> assertThat(mp2.isSuccess()).isTrue())
		            .then(() -> assertThat(mp2.peek()).isNull())
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyComplete();
	}

	@Test
	public void filterMonoProcessorError() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		StepVerifier.create(mp.filter(s -> {throw new RuntimeException("test"); })
		                      .subscribeWith
				                      (mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isTrue())
		            .then(() -> assertThat(mp2.isSuccess()).isFalse())
		            .then(() -> assertThat(mp2.getError()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void doOnSuccessMonoProcessorError() {
		NextProcessor<Integer> mp = new NextProcessor<>(null);
		NextProcessor<Integer> mp2 = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		StepVerifier.create(mp.doOnSuccess(s -> {throw new RuntimeException("test"); })
		                      .doOnError(ref::set)
		                      .subscribeWith
				                      (mp2))
		            .then(() -> mp.onNext(2))
		            .then(() -> assertThat(mp2.isError()).isTrue())
		            .then(() -> assertThat(ref.get()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isSuccess()).isFalse())
		            .then(() -> assertThat(mp2.getError()).hasMessage("test"))
		            .then(() -> assertThat(mp2.isTerminated()).isTrue())
		            .verifyErrorMessage("test");
	}

	@Test
	public void fluxCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		Flux.range(1, 10)
		    .doOnCancel(cancelCounter::incrementAndGet)
		    .shareNext()
		    .subscribe();

		assertThat(cancelCounter).hasValue(1);
	}

	@Test
	public void monoNotCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		Mono.just("foo")
		    .doOnCancel(cancelCounter::incrementAndGet)
		    .subscribe();

		assertThat(cancelCounter).hasValue(0);
	}

	@Test
	public void scanProcessor() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanProcessorCancelled() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanProcessorSubscription() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
	}

	@Test
	public void scanProcessorError() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	@SuppressWarnings("deprecated")
	public void sharedMonoReusesInstance() {
		Mono<String> sharedMono = Mono.just("foo")
		                              .hide()
		                              .share();

		assertThat(sharedMono)
				.isSameAs(sharedMono.share())
				.isSameAs(sharedMono.subscribe());
	}

	@Test
	public void sharedMonoParentIsNotNull() {
		TestPublisher<String> tp = TestPublisher.create();
		Mono<String> connectedProcessor = tp.mono().share();
		connectedProcessor.subscribe();

		assertThat(Scannable.from(connectedProcessor).scan(Scannable.Attr.PARENT)).isNotNull();
	}

	@Test
	public void sharedMonoParentChain() {
		Mono<String> m = Mono.just("foo").share();
		StepVerifier.withVirtualTime(() -> m.delayElement(Duration.ofMillis(500)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void sharedMonoChainColdToHot() {
		AtomicInteger subscriptionCount = new AtomicInteger();
		Mono<String> coldToHot = Mono.just("foo")
		                             .doOnSubscribe(sub -> subscriptionCount.incrementAndGet())
		                             .share() //this actually subscribes
		                             .filter(s -> s.length() < 4);

		coldToHot.block();
		coldToHot.block();
		coldToHot.block();

		assertThat(subscriptionCount).hasValue(1);
	}

	@Test
	public void monoProcessorBlockNegativeIsImmediateTimeout() {
		long start = System.nanoTime();

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> Mono.just("foo")
				                      .delayElement(Duration.ofMillis(500))
				                      .share()
				                      .block(Duration.ofSeconds(-1)))
				.withMessage("Timeout on Mono blocking read");

		assertThat(Duration.ofNanos(System.nanoTime() - start))
				.isLessThan(Duration.ofMillis(500));
	}

	@Test
	public void monoProcessorBlockZeroIsImmediateTimeout() {
		long start = System.nanoTime();

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> Mono.just("foo")
				                      .delayElement(Duration.ofMillis(500))
				                      .share()
				                      .block(Duration.ZERO))
				.withMessage("Timeout on Mono blocking read");

		assertThat(Duration.ofNanos(System.nanoTime() - start))
				.isLessThan(Duration.ofMillis(500));
	}

	@Test
	//FIXME that should not be the case even if it works
	public void disposeBeforeValueSendsCancellationException() {
		Mono<String> processor = Mono.<String>never().share();
		AtomicReference<Throwable> e1 = new AtomicReference<>();
		AtomicReference<Throwable> e2 = new AtomicReference<>();
		AtomicReference<Throwable> e3 = new AtomicReference<>();
		AtomicReference<Throwable> late = new AtomicReference<>();

		processor.subscribe(v -> Assertions.fail("expected first subscriber to error"), e1::set);
		processor.subscribe(v -> Assertions.fail("expected second subscriber to error"), e2::set);
		processor.subscribe(v -> Assertions.fail("expected third subscriber to error"), e3::set);

		processor.subscribe().dispose();

		assertThat(e1.get()).isInstanceOf(CancellationException.class);
		assertThat(e2.get()).isInstanceOf(CancellationException.class);
		assertThat(e3.get()).isInstanceOf(CancellationException.class);

		processor.subscribe(v -> Assertions.fail("expected late subscriber to error"), late::set);
		assertThat(late.get()).isInstanceOf(CancellationException.class);
	}

	@Test
	public void scanSubscriber(){
		NextProcessor<String> processor = new NextProcessor<>(null);
		AssertSubscriber<String> subscriber = new AssertSubscriber<>();

		NextProcessor.NextInner<String> test = new NextProcessor.NextInner<>(subscriber, processor);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanTerminated() {
		Sinks.One<Integer> sinkTerminated = new NextProcessor<>(null);

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sinkTerminated.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sinkTerminated.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");
	}

	@Test
	public void scanCancelled() {
		Sinks.One<Integer> sinkCancelled = new NextProcessor<>(null);

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("pre-cancellation").isFalse();

		((NextProcessor<?>) sinkCancelled).cancel();

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	public void inners() {
		Sinks.One<Integer> sink = new NextProcessor<>(null);
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asMono().subscribe(notScannable);
		sink.asMono().subscribe(scannable);

		assertThat(sink.inners())
				.asList()
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((NextProcessor.NextInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}
}
