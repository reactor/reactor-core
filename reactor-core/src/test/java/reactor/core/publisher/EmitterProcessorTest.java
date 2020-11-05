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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static reactor.core.Scannable.Attr;
import static reactor.core.Scannable.Attr.BUFFERED;
import static reactor.core.Scannable.Attr.CANCELLED;
import static reactor.core.Scannable.Attr.CAPACITY;
import static reactor.core.Scannable.Attr.PREFETCH;
import static reactor.core.Scannable.Attr.TERMINATED;
import static reactor.core.scheduler.Schedulers.DEFAULT_POOL_SIZE;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

/**
 * @author Stephane Maldini
 */
// This is ok as this class tests the deprecated EmitterProcessor. Will be removed with it in 3.5.
@SuppressWarnings("deprecation")
public class EmitterProcessorTest {

	@Test
	public void currentSubscriberCount() {
		Sinks.Many<Integer> sink = EmitterProcessor.create();

		assertThat(sink.currentSubscriberCount()).isZero();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isOne();

		sink.asFlux().subscribe();

		assertThat(sink.currentSubscriberCount()).isEqualTo(2);
	}

	//see https://github.com/reactor/reactor-core/issues/1364
	@Test
	public void subscribeWithSyncFusionUpstreamFirst() {
		EmitterProcessor<String> processor = EmitterProcessor.create(16);

		StepVerifier.create(
				Mono.just("DATA")
				    .subscribeWith(processor)
				    .map(String::toLowerCase)
		)
		            .expectNext("data")
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));

		assertThat(processor.blockFirst()).as("later subscription").isNull();
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	public void subscribeWithSyncFusionSingle() {
		Processor<Integer, Integer> processor = EmitterProcessor.create(16);

		StepVerifier.create(processor)
		            .then(() -> Flux.just(1).subscribe(processor))
		            .expectNext(1)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	public void subscribeWithSyncFusionMultiple() {
		Processor<Integer, Integer> processor = EmitterProcessor.create(16);

		StepVerifier.create(processor)
		            .then(() -> Flux.range(1, 5).subscribe(processor))
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	//see https://github.com/reactor/reactor-core/issues/1290
	@Test
	public void subscribeWithAsyncFusion() {
		Processor<Integer, Integer> processor = EmitterProcessor.create(16);

		StepVerifier.create(processor)
		            .then(() -> Flux.range(1, 5).publishOn(Schedulers.boundedElastic()).subscribe(processor))
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify(Duration.ofSeconds(1));
	}

	@Test
	public void emitNextNullWithAsyncFusion() {
		EmitterProcessor<Integer> processor = EmitterProcessor.create();

		// Any `QueueSubscription` capable of doing ASYNC fusion
		Fuseable.QueueSubscription<Object> queueSubscription = UnicastProcessor.create();
		processor.onSubscribe(queueSubscription);

		// Expect the ASYNC fusion mode
		assertThat(processor.sourceMode).as("sourceMode").isEqualTo(Fuseable.ASYNC);

		processor.onNext(null);
		assertThatThrownBy(() -> processor.emitNext(null, FAIL_FAST)).isInstanceOf(NullPointerException.class);
	}

	@Test
	public void testColdIdentityProcessor() throws InterruptedException {
		final int elements = 10;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Processor<Integer, Integer> processor = EmitterProcessor.create(16);

		List<Integer> list = new ArrayList<>();

		processor.subscribe(new CoreSubscriber<Integer>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer integer) {
				synchronized (list) {
					list.add(integer);
				}
				latch.countDown();
				if (latch.getCount() > 0) {
					s.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				latch.countDown();
			}
		});

		Flux.range(1, 10)
		    .subscribe(processor);


		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);

		long count = latch.getCount();
		assertThat(latch.getCount())
				.as("Count > 0 : %d (%s), Running on %d CPUs", count, list, DEFAULT_POOL_SIZE)
				.isEqualTo(0L);

	}

	/*@Test
	public void test100Hot() throws InterruptedException {
		for (int i = 0; i < 10000; i++) {
			testHotIdentityProcessor();
		}
	}
*/
	@Test
	public void testHotIdentityProcessor() throws InterruptedException {
		final int elements = 10000;
		CountDownLatch latch = new CountDownLatch(elements);

		Processor<Integer, Integer> processor = EmitterProcessor.create(1024);

		EmitterProcessor<Integer> stream = EmitterProcessor.create();
		FluxSink<Integer> session = stream.sink();
		stream.subscribe(processor);

		processor.subscribe(new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(elements);
			}

			@Override
			public void onNext(Integer integer) {
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
				System.out.println("error! " + t);
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				//latch.countDown();
			}
		});

		for (int i = 0; i < elements; i++) {
			session.next(i);
		}
		//stream.then();

		latch.await(8, TimeUnit.SECONDS);

		long count = latch.getCount();
		assertThat(latch.getCount()).as("Count > 0 : %d, Running on %d CPUs", count, DEFAULT_POOL_SIZE).isEqualTo(0);

		stream.onComplete();

	}

	@Test
	public void onNextNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().onNext(null);
		});
	}

	@Test
	public void onErrorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().onError(null);
		});
	}

	@Test
	public void onSubscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().onSubscribe(null);
		});
	}

	@Test
	public void subscribeNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().subscribe((Subscriber<Object>) null);
		});
	}

	@Test
	public void normal() {
		EmitterProcessor<Integer> tp = EmitterProcessor.create();
		StepVerifier.create(tp)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
		            })
		            .expectNext(1, 2)
		            .then(() -> {
			            tp.onNext(3);
			            tp.onComplete();
		            })
		            .expectNext(3)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}

	@Test
	public void normalBackpressured() {
		EmitterProcessor<Integer> tp = EmitterProcessor.create();
		StepVerifier.create(tp, 0L)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
			            tp.onComplete();
		            })
		            .thenRequest(10L)
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}

	@Test
	public void normalAtomicRingBufferBackpressured() {
		EmitterProcessor<Integer> tp = EmitterProcessor.create(100);
		StepVerifier.create(tp, 0L)
		            .then(() -> {
			            assertThat(tp.currentSubscriberCount()).as("has subscriber").isPositive();
			            assertThat(tp.isTerminated()).as("isTerminated").isFalse();
			            assertThat(tp.getError()).as("getError").isNull();
		            })
		            .then(() -> {
			            tp.onNext(1);
			            tp.onNext(2);
			            tp.onComplete();
		            })
		            .thenRequest(10L)
		            .expectNext(1, 2)
		            .expectComplete()
		            .verify();

		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();
		assertThat(tp.isTerminated()).as("isTerminated").isTrue();
		assertThat(tp.getError()).as("getError").isNull();
	}

	@Test
	public void state(){
		EmitterProcessor<Integer> tp = EmitterProcessor.create();
		assertThat(tp.getPending()).isEqualTo(0);
		assertThat(tp.getBufferSize()).isEqualTo(Queues.SMALL_BUFFER_SIZE);
		assertThat(tp.isCancelled()).isFalse();

		assertThat(tp.inners()).isEmpty();
		assertThat(tp.currentSubscriberCount()).as("has subscriber").isZero();

		Disposable d1 = tp.subscribe();
		assertThat(tp.inners()).hasSize(1);
		assertThat(tp.currentSubscriberCount()).isPositive();

		FluxSink<Integer> s = tp.sink();

		s.next(2);
		s.next(3);
		s.next(4);
		assertThat(tp.getPending()).isEqualTo(0);
		AtomicReference<Subscription> d2 = new AtomicReference<>();
		tp.subscribe(new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				d2.set(s);
			}

			@Override
			public void onNext(Integer integer) {

			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		});
		s.next(5);
		s.next(6);
		s.next(7);
		assertThat(tp.scan(BUFFERED)).isEqualTo(3);
		assertThat(tp.isTerminated()).isFalse();
		s.complete();
		assertThat(tp.isTerminated()).isFalse();
		d1.dispose();
		d2.get().cancel();
		assertThat(tp.isTerminated()).isTrue();

		StepVerifier.create(tp)
	                .verifyComplete();

		tp.onNext(8); //noop
		EmitterProcessor<Void> empty = EmitterProcessor.create();
		empty.onComplete();
		assertThat(empty.isTerminated()).isTrue();

	}


	@Test
	public void failNullBufferSize() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			EmitterProcessor.create(0);
		});
	}

	@Test
	public void failNullNext() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().onNext(null);
		});
	}

	@Test
	public void failNullError() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			EmitterProcessor.create().onError(null);
		});
	}

	@Test
	public void failDoubleError() {
		EmitterProcessor<Integer> ep = EmitterProcessor.create();
		StepVerifier.create(ep)
	                .then(() -> {
		                assertThat(ep.getError()).isNull();
						ep.onError(new Exception("test"));
						assertThat(ep.getError()).hasMessage("test");
						ep.onError(new Exception("test2"));
	                })
	                .expectErrorMessage("test")
	                .verifyThenAssertThat()
	                .hasDroppedErrorWithMessage("test2");
	}

	@Test
	public void failCompleteThenError() {
		EmitterProcessor<Integer> ep = EmitterProcessor.create();
		StepVerifier.create(ep)
	                .then(() -> {
						ep.onComplete();
						ep.onComplete();//noop
						ep.onError(new Exception("test"));
	                })
	                .expectComplete()
	                .verifyThenAssertThat()
	                .hasDroppedErrorWithMessage("test");
	}

	@Test
	public void ignoreDoubleOnSubscribe() {
		EmitterProcessor<Integer> ep = EmitterProcessor.create();
		ep.sink();
		assertThat(ep.sink().isCancelled()).isTrue();
	}

	@Test
	public void failNegativeBufferSize() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			EmitterProcessor.create(-1);
		});
	}

	static final List<String> DATA     = new ArrayList<>();
	static final int          MAX_SIZE = 100;

	static {
		for (int i = 1; i <= MAX_SIZE; i++) {
			DATA.add("" + i);
		}
	}

	@Test
	public void testRed() {
		EmitterProcessor<String> processor = EmitterProcessor.create();
		AssertSubscriber<String> subscriber = AssertSubscriber.create(1);
		processor.subscribe(subscriber);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		subscriber.awaitAndAssertNextValues("1");
	}

	@Test
	public void testGreen() {
		EmitterProcessor<String> processor = EmitterProcessor.create();
		AssertSubscriber<String> subscriber = AssertSubscriber.create(1);
		processor.subscribe(subscriber);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);


		subscriber.awaitAndAssertNextValues("1");
	}

	@Test
	public void testHanging() {
		EmitterProcessor<String> processor = EmitterProcessor.create(2);

		AssertSubscriber<String> first = AssertSubscriber.create(0);
		processor.log("after-1").subscribe(first);

		AssertSubscriber<String> second = AssertSubscriber.create(0);
		processor.log("after-2").subscribe(second);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);

		second.request(1);
		second.assertNoValues();

		first.request(3);

		second.awaitAndAssertNextValues("1");

		second.cancel();
		first.awaitAndAssertNextValues("1", "2", "3");
		first.cancel();

		assertThat(processor.scanOrDefault(CANCELLED, false)).isTrue();
	}

	@Test
	public void testNPE() {
		EmitterProcessor<String> processor = EmitterProcessor.create(8);
		AssertSubscriber<String> first = AssertSubscriber.create(1);
		processor.log().take(1).subscribe(first);

		AssertSubscriber<String> second = AssertSubscriber.create(3);
		processor.log().subscribe(second);

		Flux.fromIterable(DATA)
		    .log()
		    .subscribe(processor);


		first.awaitAndAssertNextValues("1");


		second.awaitAndAssertNextValues("1", "2", "3");
	}

	static class MyThread extends Thread {

		private final Flux<String> processor;

		private final CyclicBarrier barrier;

		private final int n;

		private volatile Throwable lastException;

		class MyUncaughtExceptionHandler implements UncaughtExceptionHandler {

			@Override
			public void uncaughtException(Thread t, Throwable e) {
				lastException = e;
			}

		}

		public MyThread(EmitterProcessor<String> processor, CyclicBarrier barrier, int n, int index) {
			this.processor = processor.log("consuming."+index);
			this.barrier = barrier;
			this.n = n;
			setUncaughtExceptionHandler(new MyUncaughtExceptionHandler());
		}

		@Override
		public void run() {
			try {
				doRun();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		public void doRun() throws Exception {
			AssertSubscriber<String> subscriber = AssertSubscriber.create(5);
			processor.subscribe(subscriber);
			barrier.await();

			subscriber.request(3);
			subscriber.request(4);
			subscriber.request(1);

			subscriber
					.await()
					.assertValueCount(n)
					.assertComplete();
		}

		@Nullable
		public Throwable getLastException() {
			return lastException;
		}

	}

	@Test
	@Disabled
	public void testRacing() throws Exception {
		int N_THREADS = 3;
		int N_ITEMS = 8;

		EmitterProcessor<String> processor = EmitterProcessor.create(4);
		List<String> data = new ArrayList<>();
		for (int i = 1; i <= N_ITEMS; i++) {
			data.add(String.valueOf(i));
		}

		Flux.fromIterable(data)
		    .log("publishing")
		    .subscribe(processor);

		CyclicBarrier barrier = new CyclicBarrier(N_THREADS);

		MyThread threads[] = new MyThread[N_THREADS];
		for (int i = 0; i < N_THREADS; i++) {
			threads[i] = new MyThread(processor, barrier, N_ITEMS, i);
			threads[i].start();
		}

		for (int j = 0; j < N_THREADS; j++) {
			threads[j].join();
			Throwable lastException = threads[j].getLastException();
			if (lastException != null) {
				lastException.printStackTrace();
				fail("Should not have encounterd exception");
			}
		}

	}

	@Test
	public void testThreadAffinity() throws InterruptedException {
		int count = 10;
		Scheduler[] schedulers = new Scheduler[4];
		CountDownLatch[] latches = new CountDownLatch[schedulers.length];
		for (int i = 0; i < schedulers.length; i++) {
			schedulers[i] = Schedulers.newSingle("scheduler" + i + '-');
			int expectedCount = i == 1 ? count * 2 : count;
			latches[i] = new CountDownLatch(expectedCount);
		}
		EmitterProcessor<Integer> processor = EmitterProcessor.create();
		processor.publishOn(schedulers[0])
			 .share();
		processor.publishOn(schedulers[1])
				 .subscribe(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler1")).isTrue();
						latches[1].countDown();
					});
		for (int i = 0; i < count; i++)
			processor.onNext(i);
		processor.publishOn(schedulers[2])
				 .map(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler2")).isTrue();
						latches[2].countDown();
						return i;
					})
				 .publishOn(schedulers[3])
				 .doOnNext(i -> {
						assertThat(Thread.currentThread().getName().contains("scheduler3")).isTrue();
						latches[3].countDown();
					})
				 .subscribe();
		for (int i = 0; i < count; i++)
			processor.onNext(i);
		processor.onComplete();

		for (int i = 1; i < latches.length; i++)
			assertThat(latches[i].await(5, TimeUnit.SECONDS)).isTrue();
		assertThat(latches[0].getCount()).isEqualTo(count);
	}

	@Test
	public void createDefault() {
		EmitterProcessor<Integer> processor = EmitterProcessor.create();
		assertProcessor(processor, null, null);
	}

	@Test
	public void createOverrideBufferSize() {
		int bufferSize = 1024;
		EmitterProcessor<Integer> processor = EmitterProcessor.create(bufferSize);
		assertProcessor(processor, bufferSize, null);
	}

	@Test
	public void createOverrideAutoCancel() {
		boolean autoCancel = false;
		EmitterProcessor<Integer> processor = EmitterProcessor.create(autoCancel);
		assertProcessor(processor, null, autoCancel);
	}

	@Test
	public void createOverrideAll() {
		int bufferSize = 1024;
		boolean autoCancel = false;
		EmitterProcessor<Integer> processor = EmitterProcessor.create(bufferSize, autoCancel);
		assertProcessor(processor, bufferSize, autoCancel);
	}

	public void assertProcessor(EmitterProcessor<Integer> processor,
			@Nullable Integer bufferSize,
			@Nullable Boolean autoCancel) {
		int expectedBufferSize = bufferSize != null ? bufferSize : Queues.SMALL_BUFFER_SIZE;
		boolean expectedAutoCancel = autoCancel != null ? autoCancel : true;

		assertThat(processor.prefetch).isEqualTo(expectedBufferSize);
		assertThat(processor.autoCancel).isEqualTo(expectedAutoCancel);
	}

	/**
	 * Concurrent subtraction bound to 0 and Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current atomic to update
	 * @param toSub    delta to sub
	 * @return value before subscription, 0 or Long.MAX_VALUE
	 */
	public static long getAndSub(AtomicLong sequence, long toSub) {
		long r, u;
		do {
			r = sequence.get();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = Operators.subOrZero(r, toSub);
		} while (!sequence.compareAndSet(r, u));

		return r;
	}

	@Test
	public void scanMain() {
		EmitterProcessor<Integer> test = EmitterProcessor.create(123);
		assertThat(test.scan(BUFFERED)).isEqualTo(0);
		assertThat(test.scan(CANCELLED)).isFalse();
		assertThat(test.scan(PREFETCH)).isEqualTo(123);
		assertThat(test.scan(CAPACITY)).isEqualTo(123);


		Disposable d1 = test.subscribe();

		FluxSink<Integer> sink = test.sink();

		sink.next(2);
		sink.next(3);
		sink.next(4);
		assertThat(test.scan(BUFFERED)).isEqualTo(0);

		AtomicReference<Subscription> d2 = new AtomicReference<>();
		test.subscribe(new CoreSubscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				d2.set(s);
			}

			@Override
			public void onNext(Integer integer) {

			}

			@Override
			public void onError(Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		});
		sink.next(5);
		sink.next(6);
		sink.next(7);

		assertThat(test.scan(BUFFERED)).isEqualTo(3);
		assertThat(test.scan(TERMINATED)).isFalse();

		sink.complete();
		assertThat(test.scan(TERMINATED)).isFalse();

		d1.dispose();
		d2.get().cancel();
		assertThat(test.scan(TERMINATED)).isTrue();

		//other values
		assertThat(test.scan(Scannable.Attr.PARENT)).isNotNull();
		assertThat(test.scan(Attr.ERROR)).isNull();
	}

	@Test
	public void scanMainCancelled() {
		EmitterProcessor<?> test = EmitterProcessor.create(true);
		test.subscribe().dispose();

		assertThat(test.scan(CANCELLED)).isTrue();
		assertThat(test.isCancelled()).isTrue();
	}

	@Test
	public void scanMainError() {
		EmitterProcessor<?> test = EmitterProcessor.create();
		test.sink().error(new IllegalStateException("boom"));

		assertThat(test.scan(TERMINATED)).as("terminated").isTrue();
		assertThat(test.scan(Attr.ERROR)).hasMessage("boom");
	}

	@Test
	public void inners() {
		Sinks.Many<Integer> sink = EmitterProcessor.create();
		CoreSubscriber<Integer> notScannable = new BaseSubscriber<Integer>() {};
		InnerConsumer<Integer> scannable = new LambdaSubscriber<>(null, null, null, null);

		assertThat(sink.inners()).as("before subscriptions").isEmpty();

		sink.asFlux().subscribe(notScannable);
		sink.asFlux().subscribe(scannable);

		assertThat(sink.inners())
				.asList()
				.as("after subscriptions")
				.hasSize(2)
				.extracting(l -> (Object) ((EmitterProcessor.EmitterInner<?>) l).actual)
				.containsExactly(notScannable, scannable);
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	public void syncFusionFromInfiniteStream() {
		final Flux<Integer> flux = Flux.fromStream(Stream.iterate(0, i -> i + 1));
		final EmitterProcessor<Integer> processor = EmitterProcessor.create();

		StepVerifier.create(processor)
		            .then(() -> flux.subscribe(processor))
		            .thenConsumeWhile(i -> i < 10)
		            .expectNextCount(10)
		            .thenCancel()
		            .verify(Duration.ofSeconds(4));
	}

	//see https://github.com/reactor/reactor-core/issues/1528
	@Test
	public void syncFusionFromInfiniteStreamAndTake() {
		final Flux<Integer> flux = Flux.fromStream(Stream.iterate(0, i -> i + 1))
				.take(10);
		final EmitterProcessor<Integer> processor = EmitterProcessor.create();
		flux.subscribe(processor);

		StepVerifier.create(processor)
		            .expectNextCount(10)
		            .expectComplete()
		            .verify(Duration.ofSeconds(4));
	}

	@Test
	public void removeUnknownInnerIgnored() {
		EmitterProcessor<Integer> processor = EmitterProcessor.create();
		EmitterProcessor.EmitterInner<Integer> inner = new EmitterProcessor.EmitterInner<>(null, processor);
		EmitterProcessor.EmitterInner<Integer> notInner = new EmitterProcessor.EmitterInner<>(null, processor);

		processor.add(inner);
		assertThat(processor.subscribers).as("adding inner").hasSize(1);

		processor.remove(notInner);
		assertThat(processor.subscribers).as("post remove notInner").hasSize(1);

		processor.remove(inner);
		assertThat(processor.subscribers).as("post remove inner").isEmpty();
	}

	@Test
	public void currentContextDelegatesToFirstSubscriber() {
		AssertSubscriber<Object> testSubscriber1 = new AssertSubscriber<>(Context.of("key", "value1"));
		AssertSubscriber<Object> testSubscriber2 = new AssertSubscriber<>(Context.of("key", "value2"));

		EmitterProcessor<Object> emitterProcessor = new EmitterProcessor<>(false, 1);
		emitterProcessor.subscribe(testSubscriber1);
		emitterProcessor.subscribe(testSubscriber2);

		Context processorContext = emitterProcessor.currentContext();

		assertThat(processorContext.getOrDefault("key", "EMPTY")).isEqualTo("value1");
	}

	@Test
	public void tryEmitNextWithNoSubscriberFailsOnlyIfNoCapacity() {
		EmitterProcessor<Integer> emitterProcessor = EmitterProcessor.create(1);

		assertThat(emitterProcessor.tryEmitNext(1)).isEqualTo(Sinks.EmitResult.OK);
		assertThat(emitterProcessor.tryEmitNext(2)).isEqualTo(Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);

		StepVerifier.create(emitterProcessor)
		            .expectNext(1)
		            .then(() -> emitterProcessor.tryEmitComplete().orThrow())
		            .verifyComplete();
	}

	@Test
	public void tryEmitNextWithNoSubscriberFailsIfNoCapacityAndAllSubscribersCancelledAndNoAutoTermination() {
		//in case of autoCancel, removing all subscribers results in TERMINATED rather than EMPTY
		EmitterProcessor<Integer> emitterProcessor = EmitterProcessor.create(1, false);
		AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();

		emitterProcessor.subscribe(testSubscriber);

		assertThat(emitterProcessor.tryEmitNext(1)).as("emit 1, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(emitterProcessor.tryEmitNext(2)).as("emit 2, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(emitterProcessor.tryEmitNext(3)).as("emit 3, with subscriber").isEqualTo(
				Sinks.EmitResult.OK);

		testSubscriber.cancel();

		assertThat(emitterProcessor.tryEmitNext(4)).as("emit 4, without subscriber, buffered").isEqualTo(
				Sinks.EmitResult.OK);
		assertThat(emitterProcessor.tryEmitNext(5)).as("emit 5, without subscriber").isEqualTo(
				Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER);
	}

	@Test
	public void emitNextWithNoSubscriberNoCapacityKeepsSinkOpenWithBuffer() {
		EmitterProcessor<Integer> emitterProcessor = EmitterProcessor.create(1, false);
		//fill the buffer
		assertThat(emitterProcessor.tryEmitNext(1)).as("filling buffer").isEqualTo(Sinks.EmitResult.OK);
		//test proper
		//this is "discarded" but no hook can be invoked, so effectively dropped on the floor
		emitterProcessor.emitNext(2, FAIL_FAST);

		StepVerifier.create(emitterProcessor)
		            .expectNext(1)
		            .expectTimeout(Duration.ofSeconds(1))
		            .verify();
	}
}
