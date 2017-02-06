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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.test.subscriber.AssertSubscriber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 */
public class TopicProcessorTest {


	@Test
	public void testShutdownSuccessfullAfterAllDataIsRequested() throws InterruptedException {
		TopicProcessor<String> processor = TopicProcessor.create("processor", 4);
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
		TopicProcessor<String> processor = TopicProcessor.create("processor", 4);
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
		TopicProcessor<String> processor = TopicProcessor.create("processor", 4);
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
		TopicProcessor<String> processor = TopicProcessor.create("processor", 4);
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
		TopicProcessor<String> processor = TopicProcessor.create("processor", 4);
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
			TopicProcessor<?> dispatcher = TopicProcessor.create("rb-test-dispose", 16);
			dispatcher.awaitAndShutdown();
		}
	}



	@Test
	public void drainTest() throws Exception {
		final TopicProcessor<Integer> sink = TopicProcessor.create("topic");
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
			TopicProcessor<String> bc = TopicProcessor.create(es, 16);

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
		TopicProcessor<Object> processor = TopicProcessor.create("testProcessor", TEST_BUFFER_SIZE);

		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());

		processor.awaitAndShutdown();

	}

	@Test(expected = IllegalArgumentException.class)
	public void failNullBufferSize() {
		TopicProcessor.create("test", 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNonPowerOfTwo() {
		TopicProcessor.create("test", 3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failNegativeBufferSize() {
		TopicProcessor.create("test", -1);
	}


}
