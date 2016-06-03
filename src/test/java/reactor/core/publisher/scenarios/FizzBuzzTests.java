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
package reactor.core.publisher.scenarios;

import java.time.Duration;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.AbstractReactorTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.TopicProcessor;

/**
 * https://github.com/reactor/reactor/issues/500
 *
 * @author nitzanvolman
 * @author Stephane Maldini
 */
public class FizzBuzzTests extends AbstractReactorTest {


	@Test
	public void fizzTest() throws Throwable {
		int numOfItems = 1024;
		int batchSize = 8;
		final Timer timer = new Timer();
		AtomicLong globalCounter = new AtomicLong();

		Mono<List<Object>> c = Flux.create(subscriber -> {
			System.out.println("demand is " + subscriber.requestedFromDownstream());
			while (!subscriber.isCancelled()) {
					long curr = globalCounter.incrementAndGet();
					if (curr % 5 == 0 && curr % 3 == 0) subscriber.next("FizBuz "+curr+" \r\n");
					else if (curr % 3 == 0) subscriber.next("Fiz "+curr);
					else if (curr % 5 == 0) subscriber.next("Buz "+curr);
					else subscriber.next(String.valueOf(curr) + " ");

					if (globalCounter.get() > numOfItems) {
						subscriber.complete();
						return;
					}
			}
		}).log("oooo")
		                                      .flatMap((s) -> Flux.create((sub) -> timer.schedule(new TimerTask() {
			  @Override
			  public void run() {
				  sub.next(s);
				  sub.complete();
			  }
		  }, 10)))
		                                      .useCapacity(batchSize)
		                                      .log()
		                                      .take(numOfItems + 1)
		                                      .collectList();

		c.block();
	}


	@Test
	public void indexBugTest() throws InterruptedException {
		int numOfItems = 20;

		//this line causes an java.lang.ArrayIndexOutOfBoundsException unless there is a break point in ZipAction
		// .createSubscriber()
		TopicProcessor<String> ring = TopicProcessor.create("test", 1024);

//        EmitterProcessor<String> ring = EmitterProcessor.create();


		Flux<String> stream2 = ring
				.zipWith(Flux.create(s -> {
			  while (s.requestedFromDownstream() != 0) {
				  if(!s.isCancelled()) {
					  s.next(System.currentTimeMillis());
				  }
				  else{
					  break;
				  }
			  }
		  }), (t1, t2) -> String.format("%s : %s", t1, t2));

		Mono<List<String>> p = stream2
		  .doOnNext(System.out::println)
		  .buffer(numOfItems)
		  .next();

		for (int curr = 0; curr < numOfItems; curr++) {
			if (curr % 5 == 0 && curr % 3 == 0) ring.onNext("FizBuz"+curr);
			else if (curr % 3 == 0) ring.onNext("Fiz"+curr);
			else if (curr % 5 == 0) ring.onNext("Buz"+curr);
			else ring.onNext(String.valueOf(curr));
		}

		Assert.assertTrue("Has not returned list", p.block(Duration.ofSeconds(5)) !=
				null);

	}
}

