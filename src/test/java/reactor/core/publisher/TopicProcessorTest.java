/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.WaitStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * @author Stephane Maldini
 */
public class TopicProcessorTest {


	@Test
	public void testShutdownSuccessfullAfterAllDataIsRequested() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.Builder.<String>create().name("processor").bufferSize(4).build();
		Publisher<String>
				publisher = Flux.fromArray(new String[] { "1", "2", "3", "4", "5" });
		publisher.subscribe(processor);

		AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
		processor.subscribe(subscriber);

		subscriber.request(1);

		Thread.sleep(250);

		processor.shutdown();

		assertFalse(processor.awaitAndShutdown(250, TimeUnit.MILLISECONDS));

		subscriber.request(4);

		assertTrue(processor.awaitAndShutdown(250, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testForceShutdownWhileWaitingForRequest() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.Builder.<String>create().name("processor").bufferSize(4).build();
		Publisher<String> publisher = Flux.fromArray(new String[] { "1", "2", "3", "4", "5" });
		publisher.subscribe(processor);

		AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
		processor.subscribe(subscriber);

		subscriber.request(1);

		Thread.sleep(250);

		processor.forceShutdown();

		assertTrue(processor.awaitAndShutdown(1, TimeUnit.SECONDS));
	}

	@Test
	public void testForceShutdownWhileWaitingForInitialRequest() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.Builder.<String>create().name("processor").bufferSize(4).build();
		Publisher<String> publisher = new CappedPublisher(2);
		publisher.subscribe(processor);

		AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
		processor.subscribe(subscriber);

		processor.forceShutdown();

		assertTrue(processor.awaitAndShutdown(5, TimeUnit.SECONDS));
	}


	/**
	 * Publishes {@link #nItems} data items in total after that any subscription.request is no-op.
	 */
	static class CappedPublisher implements Publisher<String> {

		private Subscriber<? super String> subscriber;

		private final int nItems;

		public CappedPublisher(int nItems) {
			this.nItems = nItems;
		}

		@Override
		public void subscribe(Subscriber<? super String> s) {
			subscriber = s;
			s.onSubscribe(new Subscription() {

				private int requested;

				@Override
				public void request(long n) {
					long limit = Math.min(n, nItems - requested);

					for (int i = 0; i < limit; i++) {
						subscriber.onNext("" + i);
					}

					requested += limit;
				}

				@Override
				public void cancel() {
				}
			});
		}

	}

	@Test
	public void testForceShutdownWhileWaitingForMoreData() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.Builder.<String>create().name("processor").bufferSize(4).build();
		Publisher<String> publisher = new CappedPublisher(2);
		publisher.subscribe(processor);

		AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
		processor.subscribe(subscriber);

		subscriber.request(3);

		Thread.sleep(250);

		processor.forceShutdown();

		assertTrue(processor.awaitAndShutdown(5, TimeUnit.SECONDS));
	}

	@Test
	public void testForceShutdownAfterShutdown() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.Builder.<String>create().name("processor").bufferSize(4).build();
		Publisher<String> publisher = Flux.fromArray(new String[] { "1", "2", "3", "4", "5" });
		publisher.subscribe(processor);

		AssertSubscriber<String> subscriber = AssertSubscriber.create(0);
		processor.subscribe(subscriber);

		subscriber.request(1);

		Thread.sleep(250);

		processor.shutdown();

		assertFalse(processor.awaitAndShutdown(400, TimeUnit.MILLISECONDS));

		processor.forceShutdown();

		assertTrue(processor.awaitAndShutdown(400, TimeUnit.MILLISECONDS));
	}

	@Test
	public void testShutdown() {
		for (int i = 0; i < 1000; i++) {
			TopicProcessor<?> dispatcher = TopicProcessor.Builder.<String>create().name("rb-test-dispose").bufferSize(16).build();
			dispatcher.awaitAndShutdown();
		}
	}



	@Test
	public void drainTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.Builder.<Integer>create().name("topic").build();
		sink.onNext(1);
		sink.onNext(2);
		sink.onNext(3);

		sink.forceShutdown()
		    .subscribeWith(AssertSubscriber.create())
		    .assertComplete()
		    .assertValues(1, 2, 3);
	}

	static Subscriber<String> sub(String name, CountDownLatch latch) {
		return new Subscriber<String>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(String o) {
				latch.countDown();
				s.request(1);
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				//latch.countDown()
			}
		};
	}

	@Test
	public void chainedTopicProcessor() throws Exception{
		ExecutorService es = Executors.newFixedThreadPool(2);
		try {
			TopicProcessor<String> bc = TopicProcessor.Builder.<String>create().executor(es).bufferSize(16).build();

			int elems = 100;
			CountDownLatch latch = new CountDownLatch(elems);

			bc.subscribe(sub("spec1", latch));
			Flux.range(0, elems)
			    .map(s -> "hello " + s)
			    .subscribe(bc);

			assertTrue(latch.await(5000, TimeUnit.MILLISECONDS));
		}
		finally {
			es.shutdown();
		}
	}

	@Test
	public void testTopicProcessorGetters() {

		final int TEST_BUFFER_SIZE = 16;
		TopicProcessor<Object> processor = TopicProcessor.Builder.create().name("testProcessor").bufferSize(TEST_BUFFER_SIZE).build();

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test(expected = IllegalArgumentException.class)
	public void failNullBufferSize() {
		TopicProcessor.Builder.create().name("test").bufferSize(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNonPowerOfTwo() {
		TopicProcessor.Builder.create().name("test").bufferSize(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeBufferSize() {
		TopicProcessor.Builder.create().name("test").bufferSize(-1);
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	public void testBufferSize1Shared() throws Exception {
		TopicProcessor<String> broadcast = TopicProcessor.Builder.<String>create()
				.name("share-name")
				.bufferSize(1)
				.autoCancel(true)
				.share(true)
				.build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.next("data");

		assertThat(latch.await(4, TimeUnit.SECONDS))
				.overridingErrorMessage("Data not received")
				.isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/445
	@Test(timeout = 5_000)
	public void testBufferSize1Created() throws Exception {
		TopicProcessor<String> broadcast = TopicProcessor.Builder.<String>create().name("share-name").bufferSize(1).autoCancel(true).build();

		int simultaneousSubscribers = 3000;
		CountDownLatch latch = new CountDownLatch(simultaneousSubscribers);
		Scheduler scheduler = Schedulers.single();

		FluxSink<String> sink = broadcast.sink();
		Flux<String> flux = broadcast.filter(Objects::nonNull)
		                             .publishOn(scheduler)
		                             .cache(1);

		for (int i = 0; i < simultaneousSubscribers; i++) {
			flux.subscribe(s -> latch.countDown());
		}
		sink.next("data");

		assertThat(latch.await(4, TimeUnit.SECONDS))
				.overridingErrorMessage("Data not received")
				.isTrue();
	}


	@Test
	public void testDefaultRequestTaskThreadName() {
		String mainName = "topicProcessorRequestTask";
		String expectedName = mainName + "[request-task]";

		TopicProcessor<Object> processor = TopicProcessor.Builder.create().name(mainName).bufferSize(8).build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		processor.forceShutdown();


		Condition<Thread> defaultRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, defaultRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadName() {
		String expectedName = "topicProcessorRequestTaskCreate";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));
		TopicProcessor<Object> processor = TopicProcessor.Builder.create()
				.executor(Executors.newCachedThreadPool())
				.requestTaskExecutor(customTaskExecutor)
				.bufferSize(8)
				.waitStrategy(WaitStrategy.liteBlocking())
				.autoCancel(true)
				.build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		Condition<Thread> customRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void testCustomRequestTaskThreadShare() {
		String expectedName = "topicProcessorRequestTaskShare";
		//NOTE: the below single executor should not be used usually as requestTask assumes it immediately gets executed
		ExecutorService customTaskExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, expectedName));

		TopicProcessor<Object> processor = TopicProcessor.Builder.create().share(true)
				.executor(Executors.newCachedThreadPool())
				.requestTaskExecutor(customTaskExecutor)
				.bufferSize(8)
				.waitStrategy(WaitStrategy.liteBlocking())
				.autoCancel(true)
				.build();

		processor.requestTask(Operators.cancelledSubscription());

		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);

		//cleanup to avoid visibility in other tests
		customTaskExecutor.shutdownNow();
		processor.forceShutdown();

		Condition<Thread> customRequestTaskThread = new Condition<>(
				thread -> expectedName.equals(thread.getName()),
				"a thread named \"%s\"", expectedName);

		Assertions.assertThat(threads)
		          .haveExactly(1, customRequestTaskThread);
	}

	@Test
	public void customRequestTaskThreadRejectsNull() {
		ExecutorService customTaskExecutor = null;

		Assertions.assertThatExceptionOfType(NullPointerException.class)
		          .isThrownBy(() -> new TopicProcessor<>(
				          Thread::new,
				          Executors.newCachedThreadPool(),
				          customTaskExecutor,
				          8, WaitStrategy.liteBlocking(), true, true, Object::new)
		          );
	}



	@Test
	public void createDefault() {
		TopicProcessor<Integer> processor = TopicProcessor.create();
		assertProcessor(processor, false, null, null, null, null, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideAutoCancel() {
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(autoCancel);
		assertProcessor(processor, false, null, null, null, autoCancel, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideName() {
		String name = "nameOverride";
		TopicProcessor<Integer> processor = TopicProcessor.create(name);
		assertProcessor(processor, false, name, null, null, null, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.create(name, bufferSize);
		assertProcessor(processor, false, name, bufferSize, null, null, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideNameBufferSizeAutoCancel() {
		String name = "nameOverride";
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(name, bufferSize, autoCancel);
		assertProcessor(processor, false, name, bufferSize, null, autoCancel, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.create(name, bufferSize, waitStrategy);
		assertProcessor(processor, false, name, bufferSize, waitStrategy, null, null, null);
	}

	@Test
	@Deprecated
	public void createDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(name, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, false, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		TopicProcessor<Integer> processor = TopicProcessor.create(executor);
		assertProcessor(processor, false, null, null, null, null, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, autoCancel);
		assertProcessor(processor, false, null, null, null, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, bufferSize);
		assertProcessor(processor, false, null, bufferSize, null, null, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, bufferSize, autoCancel);
		assertProcessor(processor, false, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, bufferSize, waitStrategy);
		assertProcessor(processor, false, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void createOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.create(executor, requestTaskExecutor, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, false, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	@Deprecated
	public void shareOverrideAutoCancel() {
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(autoCancel);
		assertProcessor(processor, true, null, null, null, autoCancel, null, null);
	}

	@Test
	@Deprecated
	public void shareOverrideNameBufferSize() {
		String name = "nameOverride";
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.share(name, bufferSize);
		assertProcessor(processor, true, name, bufferSize, null, null, null, null);
	}

	@Test
	@Deprecated
	public void shareOverrideNameBufferSizeWaitStrategy() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.share(name, bufferSize, waitStrategy);
		assertProcessor(processor, true, name, bufferSize, waitStrategy, null, null, null);
	}

	@Test
	@Deprecated
	public void shareDefaultExecutorOverrideAll() {
		String name = "nameOverride";
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(name, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, true, name, bufferSize, waitStrategy, autoCancel, null, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutor() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		TopicProcessor<Integer> processor = TopicProcessor.share(executor);
		assertProcessor(processor, true, null, null, null, null, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutorAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, autoCancel);
		assertProcessor(processor, true, null, null, null, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutorBufferSize() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, bufferSize);
		assertProcessor(processor, true, null, bufferSize, null, null, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutorBufferSizeAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, bufferSize, autoCancel);
		assertProcessor(processor, true, null, bufferSize, null, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutorBufferSizeWaitStrategy() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, bufferSize, waitStrategy);
		assertProcessor(processor, true, null, bufferSize, waitStrategy, null, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideExecutorBufferSizeWaitStrategyAutoCancel() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, null);
	}

	@Test
	@Deprecated
	public void shareOverrideAll() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		ExecutorService requestTaskExecutor = Executors.newSingleThreadExecutor();
		int bufferSize = 1024;
		WaitStrategy waitStrategy = WaitStrategy.busySpin();
		boolean autoCancel = false;
		TopicProcessor<Integer> processor = TopicProcessor.share(executor, requestTaskExecutor, bufferSize, waitStrategy, autoCancel);
		assertProcessor(processor, true, null, bufferSize, waitStrategy, autoCancel, executor, requestTaskExecutor);
	}

	@Test
	public void scanProcessor() {
		TopicProcessor<String> test = TopicProcessor.create("name", 16);
		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isEqualTo(subscription);

		assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(16);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).hasMessage("boom");
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanInner() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		Subscriber<String> activated = new LambdaSubscriber<>(null, e -> {}, null, null);

		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(
				main, sequence, activated);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(main);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(activated);
		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123L);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		main.terminated = 1;
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}


	@Test
	public void scanInnerBufferedSmallHasIntRealValue() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		Subscriber<String> sub = new LambdaSubscriber<>(null, e -> {}, null, null);
		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(main, sequence, sub);

		main.ringBuffer.getSequencer().cursor.set(Integer.MAX_VALUE + 5L);
		test.sequence.set(6L);

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(Integer.MAX_VALUE - 1);
		assertThat(test.scan(Scannable.LongAttr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE - 1L);
	}

	@Test
	public void scanInnerBufferedLargeHasIntMinValue() {
		TopicProcessor<String> main = TopicProcessor.create("name", 16);
		RingBuffer.Sequence sequence = RingBuffer.newSequence(123);
		Subscriber<String> sub = new LambdaSubscriber<>(null, e -> {}, null, null);
		TopicProcessor.TopicInner<String> test = new TopicProcessor.TopicInner<>(main, sequence, sub);

		main.ringBuffer.getSequencer().cursor.set(Integer.MAX_VALUE + 5L);
		test.sequence.set(2L);

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(Integer.MIN_VALUE);
		assertThat(test.scan(Scannable.LongAttr.LARGE_BUFFERED)).isEqualTo(Integer.MAX_VALUE + 3L);
	}

	private void assertProcessor(TopicProcessor<Integer> processor,
			boolean shared,
			String name,
			Integer bufferSize,
			WaitStrategy waitStrategy,
			Boolean autoCancel,
			ExecutorService executor,
			ExecutorService requestTaskExecutor) {

		String expectedName = name != null ? name : TopicProcessor.class.getSimpleName();
		int expectedBufferSize = bufferSize != null ? bufferSize : QueueSupplier.SMALL_BUFFER_SIZE;
		boolean expectedAutoCancel = autoCancel != null ? autoCancel : true;
		WaitStrategy expectedWaitStrategy = waitStrategy != null ? waitStrategy : WaitStrategy.phasedOffLiteLock(200, 100, TimeUnit.MILLISECONDS);
		Class<?> sequencerClass = shared ? MultiProducerRingBuffer.class : SingleProducerSequencer.class;

		assertEquals(expectedName, processor.name);
		assertEquals(expectedBufferSize, processor.getBufferSize());
		assertEquals(expectedAutoCancel, processor.autoCancel);
		assertEquals(expectedWaitStrategy.getClass(), processor.ringBuffer.getSequencer().waitStrategy.getClass());
		assertEquals(sequencerClass, processor.ringBuffer.getSequencer().getClass());
		if (executor != null)
			assertEquals(executor, processor.executor);
		if (requestTaskExecutor != null)
			assertEquals(requestTaskExecutor, processor.requestTaskExecutor);
	}
}
