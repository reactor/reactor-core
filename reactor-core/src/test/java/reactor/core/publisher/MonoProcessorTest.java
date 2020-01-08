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
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoProcessorTest {

	@Test
	public void noRetentionOnTermination() throws InterruptedException {
		Date date = new Date();
		CompletableFuture<Date> future = new CompletableFuture<>();

		WeakReference<Date> refDate = new WeakReference<>(date);
		WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

		Mono<Date> source = Mono.fromFuture(future);
		Mono<String> data = source.map(Date::toString).as(MonoProcessor::new);

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
		Mono<String> data = source.map(Date::toString).as(MonoProcessor::new);

		future.completeExceptionally(new IllegalStateException());

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(data::block);

		future = null;
		source = null;
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
	public void noRetentionOnTerminationCancel() throws InterruptedException {
		CompletableFuture<Date> future = new CompletableFuture<>();

		WeakReference<CompletableFuture<Date>> refFuture = new WeakReference<>(future);

		Mono<Date> source = Mono.fromFuture(future);
		Mono<String> data = source.map(Date::toString).as(MonoProcessor::new);

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

	@Test(expected = IllegalStateException.class)
	public void MonoProcessorResultNotAvailable() {
		MonoProcessor<String> mp = MonoProcessor.create();
		mp.block(Duration.ofMillis(1));
	}

	@Test
	public void MonoProcessorRejectedDoOnSuccessOrError() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(f)).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorRejectedDoOnTerminate() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe();
		mp.onError(new Exception("test"));

		assertThat(invoked.get()).isEqualTo(1);
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorRejectedSubscribeCallback() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.subscribe(v -> {}, ref::set);
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorSuccessDoOnSuccessOrError() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccessOrError((s, f) -> ref.set(s)).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorSuccessDoOnTerminate() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicInteger invoked = new AtomicInteger();

		mp.doOnTerminate(invoked::incrementAndGet).subscribe();
		mp.onNext("test");

		assertThat(invoked.get()).isEqualTo(1);
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorSuccessSubscribeCallback() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.subscribe(ref::set);
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorRejectedDoOnError() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<Throwable> ref = new AtomicReference<>();

		mp.doOnError(ref::set).subscribe();
		mp.onError(new Exception("test"));

		assertThat(ref.get()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test(expected = NullPointerException.class)
	public void MonoProcessorRejectedSubscribeCallbackNull() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.subscribe((Subscriber<String>)null);
	}

	@Test
	public void MonoProcessorSuccessDoOnSuccess() {
		MonoProcessor<String> mp = MonoProcessor.create();
		AtomicReference<String> ref = new AtomicReference<>();

		mp.doOnSuccess(ref::set).subscribe();
		mp.onNext("test");

		assertThat(ref.get()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorSuccessChainTogether() {
		MonoProcessor<String> mp = MonoProcessor.create();
		MonoProcessor<String> mp2 = MonoProcessor.create();
		mp.subscribe(mp2);

		mp.onNext("test");

		assertThat(mp2.peek()).isEqualToIgnoringCase("test");
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.isError()).isFalse();
	}

	@Test
	public void MonoProcessorRejectedChainTogether() {
		MonoProcessor<String> mp = MonoProcessor.create();
		MonoProcessor<String> mp2 = MonoProcessor.create();
		mp.subscribe(mp2);

		mp.onError(new Exception("test"));

		assertThat(mp2.getError()).hasMessage("test");
		assertThat(mp.isSuccess()).isFalse();
		assertThat(mp.isError()).isTrue();
	}

	@Test
	public void MonoProcessorDoubleFulfill() {
		MonoProcessor<String> mp = MonoProcessor.create();

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
	public void MonoProcessorNullFulfill() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext(null);

		assertThat(mp.isTerminated()).isTrue();
		assertThat(mp.isSuccess()).isTrue();
		assertThat(mp.peek()).isNull();
	}

	@Test
	public void MonoProcessorMapFulfill() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = mp.map(s -> s * 2)
		                               .toProcessor();
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void MonoProcessorThenFulfill() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = mp.flatMap(s -> Mono.just(s * 2))
		                               .toProcessor();
		mp2.subscribe();

		assertThat(mp2.isTerminated()).isTrue();
		assertThat(mp2.isSuccess()).isTrue();
		assertThat(mp2.peek()).isEqualTo(2);
	}

	@Test
	public void MonoProcessorMapError() {
		MonoProcessor<Integer> mp = MonoProcessor.create();

		mp.onNext(1);

		MonoProcessor<Integer> mp2 = MonoProcessor.create();

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

	@Test(expected = Exception.class)
	public void MonoProcessorDoubleError() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onError(new Exception("test"));
		mp.onError(new Exception("test"));
	}

	@Test(expected = Exception.class)
	public void MonoProcessorDoubleSignal() {
		MonoProcessor<String> mp = MonoProcessor.create();

		mp.onNext("test");
		mp.onError(new Exception("test"));
	}

	@Test
	public void zipMonoProcessor() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		MonoProcessor<Tuple2<Integer, Integer>> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.zip(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onNext(1))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp2.onNext(2))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.isPending()).isFalse();
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
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.zip(d -> (Integer)d[0], mp)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onNext(1))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isTrue();
			            assertThat(mp3.isPending()).isFalse();
			            assertThat(mp3.peek()).isEqualTo(1);
		            })
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void zipMonoProcessorRejected() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
		MonoProcessor<Tuple2<Integer, Integer>> mp3 = MonoProcessor.create();

		StepVerifier.create(Mono.zip(mp, mp2)
		                        .subscribeWith(mp3))
		            .then(() -> assertThat(mp3.isPending()).isTrue())
		            .then(() -> mp.onError(new Exception("test")))
		            .then(() -> {
			            assertThat(mp3.isTerminated()).isTrue();
			            assertThat(mp3.isSuccess()).isFalse();
			            assertThat(mp3.isPending()).isFalse();
			            assertThat(mp3.getError()).hasMessage("test");
		            })
		            .verifyErrorMessage("test");
	}


	@Test
	public void filterMonoProcessor() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
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
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
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
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
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
		MonoProcessor<Integer> mp = MonoProcessor.create();
		MonoProcessor<Integer> mp2 = MonoProcessor.create();
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
		    .publishNext()
		    .subscribe();

		assertThat(cancelCounter.get()).isEqualTo(1);
	}

	@Test
	public void monoNotCancelledByMonoProcessor() {
		AtomicLong cancelCounter = new AtomicLong();
		MonoProcessor<String> monoProcessor = Mono.just("foo")
		                                          .doOnCancel(cancelCounter::incrementAndGet)
		                                          .toProcessor();
		monoProcessor.subscribe();

		assertThat(cancelCounter.get()).isEqualTo(0);
	}

	@Test
	public void scanProcessor() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
	}

	@Test
	public void scanProcessorCancelled() {
		MonoProcessor<String> test = MonoProcessor.create();
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
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(subscription);
	}

	@Test
	public void scanProcessorError() {
		MonoProcessor<String> test = MonoProcessor.create();
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		test.onError(new IllegalStateException("boom"));

		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void monoToProcessorReusesInstance() {
		MonoProcessor<String> monoProcessor = Mono.just("foo")
		                                          .toProcessor();

		assertThat(monoProcessor)
				.isSameAs(monoProcessor.toProcessor())
				.isSameAs(monoProcessor.subscribe());
	}

	@Test
	public void monoToProcessorConnects() {
		TestPublisher<String> tp = TestPublisher.create();
		MonoProcessor<String> connectedProcessor = tp.mono().toProcessor();

		assertThat(connectedProcessor.subscription).isNotNull();
	}

	@Test
	public void monoToProcessorChain() {
		StepVerifier.withVirtualTime(() -> Mono.just("foo")
		                                       .toProcessor()
		                                       .delayElement(Duration.ofMillis(500)))
		            .expectSubscription()
		            .expectNoEvent(Duration.ofMillis(500))
		            .expectNext("foo")
		            .verifyComplete();
	}

	@Test
	public void monoToProcessorChainColdToHot() {
		AtomicInteger subscriptionCount = new AtomicInteger();
		Mono<String> coldToHot = Mono.just("foo")
		                             .doOnSubscribe(sub -> subscriptionCount.incrementAndGet())
		                             .toProcessor() //this actually subscribes
		                             .filter(s -> s.length() < 4);

		assertThat(subscriptionCount.get()).isEqualTo(1);

		coldToHot.block();
		coldToHot.block();
		coldToHot.block();

		assertThat(subscriptionCount.get()).isEqualTo(1);
	}

	@Test
	public void monoProcessorBlockIsUnbounded() {
		long start = System.nanoTime();

		String result = Mono.just("foo")
		                    .delayElement(Duration.ofMillis(500))
		                    .toProcessor()
		                    .block();

		assertThat(result).isEqualTo("foo");
		assertThat(Duration.ofNanos(System.nanoTime() - start))
				.isGreaterThanOrEqualTo(Duration.ofMillis(500));
	}

	@Test
	public void monoProcessorBlockNegativeIsImmediateTimeout() {
		long start = System.nanoTime();

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> Mono.just("foo")
				                      .delayElement(Duration.ofMillis(500))
				                      .toProcessor()
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
				                      .toProcessor()
				                      .block(Duration.ZERO))
		        .withMessage("Timeout on Mono blocking read");

		assertThat(Duration.ofNanos(System.nanoTime() - start))
				.isLessThan(Duration.ofMillis(500));
	}

	@Test
	public void disposeBeforeValueSendsCancellationException() {
		MonoProcessor<String> processor = MonoProcessor.create();
		AtomicReference<Throwable> e1 = new AtomicReference<>();
		AtomicReference<Throwable> e2 = new AtomicReference<>();
		AtomicReference<Throwable> e3 = new AtomicReference<>();
		AtomicReference<Throwable> late = new AtomicReference<>();

		processor.subscribe(v -> Assertions.fail("expected first subscriber to error"), e1::set);
		processor.subscribe(v -> Assertions.fail("expected second subscriber to error"), e2::set);
		processor.subscribe(v -> Assertions.fail("expected third subscriber to error"), e3::set);

		processor.dispose();

		assertThat(e1.get()).isInstanceOf(CancellationException.class);
		assertThat(e2.get()).isInstanceOf(CancellationException.class);
		assertThat(e3.get()).isInstanceOf(CancellationException.class);

		processor.subscribe(v -> Assertions.fail("expected late subscriber to error"), late::set);
		assertThat(late.get()).isInstanceOf(CancellationException.class);
	}
}