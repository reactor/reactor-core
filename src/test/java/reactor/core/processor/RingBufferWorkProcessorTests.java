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

package reactor.core.processor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Processor;
import reactor.Flux;
import reactor.Processors;
import reactor.Subscribers;
import reactor.core.publisher.FluxLift;
import reactor.core.support.Assert;

/**
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class RingBufferWorkProcessorTests extends AbstractProcessorVerification {

	@Override
	public Processor<Long, Long> createProcessor(int bufferSize) {
		System.out.println("new processor");
		return Processors.blackbox(Processors.<Long>queue("rb-work", bufferSize), Flux::log);
	}

	@Override
	public void required_mustRequestFromUpstreamForElementsThatHaveBeenRequestedLongAgo()
			throws Throwable {
		//IGNORE since subscribers see distinct data
	}

	@Override
	public void simpleTest() throws Exception {
		final FluxProcessor<Integer, Integer> sink = Processors.topic("topic");
		final FluxProcessor<Integer, Integer> processor = Processors.queue("queue");

		int elems = 1_000_000;
		CountDownLatch latch = new CountDownLatch(elems);

		//List<Integer> list = new CopyOnWriteArrayList<>();
		AtomicLong count = new AtomicLong();
		AtomicLong errorCount = new AtomicLong();

		processor.subscribe(Subscribers.unbounded((d, sub) -> {
			errorCount.incrementAndGet();
			sub.abort();
		}));

		Flux.from(processor).doOnNext(
			d -> count.incrementAndGet()
		).subscribe(Subscribers.unbounded((d, sub) -> {
			latch.countDown();
			//list.add(d);
		}));

		sink.subscribe(processor);
		sink.start();
		for(int i = 0; i < elems; i++){

			sink.onNext(i);
			if( i % 100 == 0) {
				processor.subscribe(Subscribers.unbounded((d, sub) -> {
					errorCount.incrementAndGet();
					sub.abort();
				}));
			}
		}

		latch.await(5, TimeUnit.SECONDS);
		System.out.println("count " + count+" errors: "+errorCount);
		sink.onComplete();
 		Assert.isTrue(latch.getCount() <= 1, "Latch is " + latch.getCount());


	}

	/*public static void main() {
		final RingBufferWorkProcessor<Long> processor = RingBufferWorkProcessor.<Long>create("some-test");

		Publisher<Long> pub = PublisherFactory.create(
		  c -> {
			  if (c.context().incrementAndGet() >= 661) {
				  c.onComplete();
				  processor.onComplete();
			  } else {
				  try {
					  Thread.sleep(50);
				  } catch (InterruptedException e) {

				  }
				  c.onNext(c.context().get());
				  System.out.println(c.context() + " emit");
			  }
		  },
		  s -> new AtomicLong()
		);

		for (int i = 0; i < 2; i++) {
			processor.subscribe(new Subscriber<Long>() {
				@Override
				public void onSubscribe(Subscription s) {
					s.request(1000);
				}

				@Override
				public void onNext(Long aLong) {
					System.out.println(Thread.currentThread() + " next " + aLong);
				}

				@Override
				public void onError(Throwable t) {

				}

				@Override
				public void onComplete() {
					System.out.println("finish");
				}
			});
		}

		processor
		  .writeWith(pub)
		  .subscribe(Subscribers.unbounded());
	}*/
}
