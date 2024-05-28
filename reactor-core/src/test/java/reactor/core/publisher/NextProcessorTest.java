/*
 * Copyright (c) 2015-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.TestLoggerExtension;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.LoggerUtils;
import reactor.test.util.TestLogger;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class NextProcessorTest {

	@Test
	void monoShareIsReferenceCounted() {
		AtomicBoolean cancelled = new AtomicBoolean();
		Mono<Object> mono = Mono.never()
			.doOnCancel(() -> cancelled.set(true))
			.share();

		mono
			.subscribe()
			.dispose();

		assertThat(cancelled).isTrue();
	}

	@Test
	void monoShareIsReferenceCounted2() {
		AtomicInteger cancelledCount = new AtomicInteger();
		AtomicInteger source = new AtomicInteger();

		Mono<Integer> mono = Mono.fromCallable(source::incrementAndGet)
			.delayElement(Duration.ofSeconds(10))
			.doOnCancel(cancelledCount::incrementAndGet)
			.share();

		mono.subscribe().dispose();
		mono.subscribe(v -> {}).dispose();
		mono.subscribe(v -> {}, Throwable::printStackTrace).dispose();
		mono.subscribe().dispose();

		assertThat(source).hasValue(4);
		assertThat(cancelledCount).hasValue(4);
	}

	@Test
	void monoShareSubscribeNoArgsCanBeIndividuallyDisposed() {
		AtomicInteger cancelledCount = new AtomicInteger();

		Mono<Object> mono = Mono.never()
			.doOnCancel(cancelledCount::incrementAndGet)
			.share();

		Disposable sub1 = mono.subscribe();
		Disposable sub2 = mono.subscribe();

		assertThat(sub1).as("subscription are differentiated").isNotSameAs(sub2);

		sub1.dispose();

		assertThat(cancelledCount).as("not cancelled when 1+ subscriber").hasValue(0);

		AtomicBoolean sub3Terminated = new AtomicBoolean();
		Disposable sub3 = mono.doOnTerminate(() -> sub3Terminated.set(true)).subscribe();

		assertThat(sub3).as("3rd subscribe differentiated").isNotSameAs(sub2);
		assertThat(sub3Terminated).as("subscription still possible once sub disposed").isFalse();

		sub2.dispose();
		sub3.dispose();

		Scannable.from(mono).inners().forEach(System.out::println);

		assertThat(cancelledCount).as("cancelled once 0 subscriber").hasValue(1);

		AtomicBoolean sub4Terminated = new AtomicBoolean();

		Disposable sub4 = mono.doOnTerminate(() -> sub4Terminated.set(true)).subscribe();

		assertThat(sub4Terminated)
			.as("subscription still possible once all subs disposed")
			.isFalse();
	}

	@Test
	void shareCanDisconnectAndReconnect() {
		AtomicInteger connectionCount = new AtomicInteger();
		AtomicInteger disconnectionCount = new AtomicInteger();

		Mono<Object> shared = Mono.never()
			.doFirst(() -> connectionCount.incrementAndGet())
			.doOnCancel(() -> disconnectionCount.incrementAndGet())
			.share();

		Disposable sub1 = shared.subscribe();

		assertThat(connectionCount).as("sub1 connectionCount").hasValue(1);
		assertThat(disconnectionCount).as("sub1 disconnectionCount before dispose").hasValue(0);

		sub1.dispose();

		assertThat(disconnectionCount).as("sub1 disconnectionCount after dispose").hasValue(1);

		Disposable sub2 = shared.subscribe();

		assertThat(connectionCount).as("sub2 connectionCount").hasValue(2);
		assertThat(disconnectionCount).as("sub2 disconnectionCount before dispose").hasValue(1);

		sub2.dispose();

		assertThat(disconnectionCount).as("sub2 disconnectionCount after dispose").hasValue(2);
	}

	@Test
	void downstreamCount() {
		//using never() as source, otherwise subscribers get immediately completed and removed
		NextProcessor<Integer> processor = new NextProcessor<>(Flux.never());

		assertThat(processor.downstreamCount()).isZero();

		processor.subscribe();
		assertThat(processor.downstreamCount()).isOne();

		processor.subscribe();
		assertThat(processor.downstreamCount()).isEqualTo(2);
	}

	@Test
	void noRetentionOnTermination() throws InterruptedException {
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
	void noRetentionOnTerminationError() throws InterruptedException {
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
	void noRetentionOnTerminationCancel() throws InterruptedException {
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
	void MonoProcessorResultNotAvailable() {
		assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> {
			NextProcessor<String> mp = new NextProcessor<>(null);
			mp.block(Duration.ofMillis(1));
		});
	}

	@Test
	void rejectedDoOnTerminate() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(invoked).hasValue(1);
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void rejectedSubscribeCallback() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void successDoOnTerminate() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe();
		mp.onNext("test");

		assertThat(invoked).hasValue(1);
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	void successSubscribeCallback() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<String> ref = new AtomicReference<>();

		mp.subscribe(ref::set);
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	void rejectedDoOnError() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe(v -> {}, e -> {});
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void rejectedSubscribeCallbackNull() {
		NextProcessor<String> mp = new NextProcessor<>(null);

		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			mp.subscribe((Subscriber<String>) null);
		});
	}

	@Test
	void successDoOnSuccess() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccess(ref::set).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	void successChainTogether() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		NextProcessor<String> mp2 = new NextProcessor<>(null);
		mp.subscribe(mp2);

		mp.onNext("test");

		assertThat(mp2.peek()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	void rejectedChainTogether() {
		NextProcessor<String> mp = new NextProcessor<>(null);
		NextProcessor<String> mp2 = new NextProcessor<>(null);
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	void doubleFulfill() {
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
	void nullFulfill() {
		NextProcessor<String> mp = new NextProcessor<>(null);

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	void mapFulfill() {
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
	void thenFulfill() {
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
	void mapError() {
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
	@TestLoggerExtension.Redirect
	void doubleError(TestLogger testLogger) {
		NextProcessor<String> mp = new NextProcessor<>(null);

		mp.onError(new Exception("test"));
		mp.onError(new Exception("test2"));
		Assertions.assertThat(testLogger.getErrContent())
			.contains("Operator called default onErrorDropped")
			.contains("test2");
	}

	@Test
	@TestLoggerExtension.Redirect
	void doubleSignal(TestLogger testLogger) {
		NextProcessor<String> mp = new NextProcessor<>(null);

		mp.onNext("test");
		mp.onError(new Exception("test2"));

		Assertions.assertThat(testLogger.getErrContent())
			.contains("Operator called default onErrorDropped")
			          .contains("test2");
	}

	@Test
	void zipMonoProcessor() {
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
	void zipMonoProcessor2() {
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
	void zipMonoProcessorRejected() {
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
	void filterMonoProcessor() {
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
	void filterMonoProcessorNot() {
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
	void filterMonoProcessorError() {
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
	void doOnSuccessMonoProcessorError() {
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
	void fluxCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		Flux.range(1, 10)
		    .doOnCancel(cancelCounter::incrementAndGet)
		    .shareNext()
		    .subscribe();

		assertThat(cancelCounter).hasValue(1);
	}

	@Test
	void monoNotCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		Mono.just("foo")
		    .doOnCancel(cancelCounter::incrementAndGet)
		    .subscribe();

		assertThat(cancelCounter).hasValue(0);
	}

	@Test
	void scanProcessor() {
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
	void scanProcessorCancelled() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.doCancel();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void scanProcessorSubscription() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
	}

	@Test
	void scanProcessorError() {
		NextProcessor<String> test = new NextProcessor<>(null);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	void sharingSharedMonoIsSameInstance() {
		Mono<String> sharedMono = Mono.just("foo")
		                              .hide()
		                              .share();

		assertThat(sharedMono.share())
			.isSameAs(sharedMono);
	}

	@Test
	void sharingNextProcessorIsNewInstance() {
		NextProcessor<String> nextProcessor = new NextProcessor<>(null, false);
		assertThat(nextProcessor.share()).isNotSameAs(nextProcessor);
	}

	@Test
	void sharedMonoParentIsNotNull() {
		TestPublisher<String> tp = TestPublisher.create();
		Mono<String> connectedProcessor = tp.mono().share();
		connectedProcessor.subscribe();

		assertThat(Scannable.from(connectedProcessor).scan(Scannable.Attr.PARENT)).isNotNull();
	}

	@Test
	void sharedMonoParentChain() {
		Mono<String> m = Mono.just("foo").share();
		StepVerifier.withVirtualTime(() -> m.delayElement(Duration.ofMillis(500)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	void sharedMonoChainColdToHot() {
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
	void monoProcessorBlockNegativeIsImmediateTimeout() {
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
	void monoProcessorBlockZeroIsImmediateTimeout() {
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
	void sharedDisposeBeforeValueDoesNotDisposeProcessor() {
		AtomicInteger cancellationErrorCount = new AtomicInteger();
		Mono<String> processor = Mono.<String>never().share();

		processor.subscribe(v -> Assertions.fail("expected first subscriber to receive nothing"), e1 -> cancellationErrorCount.incrementAndGet());
		processor.subscribe(v -> Assertions.fail("expected second subscriber to receive nothing"), e2 -> cancellationErrorCount.incrementAndGet());
		processor.subscribe(v -> Assertions.fail("expected third subscriber to receive nothing"), e3 -> cancellationErrorCount.incrementAndGet());

		processor.subscribe().dispose();

		assertThat(cancellationErrorCount).as("before late subscribe").hasValue(0);

		processor.subscribe(v -> Assertions.fail("expected late subscriber to receive nothing"), late -> cancellationErrorCount.incrementAndGet());

		assertThat(cancellationErrorCount).as("after late subscribe").hasValue(0);
	}

	@Test
	void scanSubscriber(){
		NextProcessor<String> processor = new NextProcessor<>(null);
		AssertSubscriber<String> subscriber = new AssertSubscriber<>();

		NextProcessor.NextInner<String> test = new NextProcessor.NextInner<>(subscriber, processor);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanTerminated() {
		NextProcessor<Integer> sinkTerminated = new NextProcessor<>(null);

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("not yet terminated").isFalse();

		sinkTerminated.tryEmitError(new IllegalStateException("boom")).orThrow();

		assertThat(sinkTerminated.scan(Scannable.Attr.TERMINATED)).as("terminated with error").isTrue();
		assertThat(sinkTerminated.scan(Scannable.Attr.ERROR)).as("error").hasMessage("boom");
	}

	@Test
	void scanCancelled() {
		NextProcessor<Integer> sinkCancelled = new NextProcessor<>(null);

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("pre-cancellation").isFalse();

		sinkCancelled.doCancel();

		assertThat(sinkCancelled.scan(Scannable.Attr.CANCELLED)).as("cancelled").isTrue();
	}

	@Test
	void inners() {
		NextProcessor<Integer> sink = new NextProcessor<>(null);
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.subscribe(notScannable);
		sink.subscribe(scannable);

		assertThat(sink.inners())
				.asInstanceOf(InstanceOfAssertFactories.LIST)
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((NextProcessor.NextInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}
}
