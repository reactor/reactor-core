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
package reactor.core.publisher.scenarios;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
//		int batchSize = 8;
		final Timer timer = new Timer();
		AtomicLong globalCounter = new AtomicLong();

		Mono<List<Object>> c = Flux.create(subscriber -> {
			for (; ; ) {
				long curr = globalCounter.incrementAndGet();
				if (curr % 5 == 0 && curr % 3 == 0) {
					subscriber.next("FizBuz " + curr + " \r\n");
				}
				else if (curr % 3 == 0) {
					subscriber.next("Fiz " + curr);
				}
				else if (curr % 5 == 0) {
					subscriber.next("Buz " + curr);
				}
				else {
					subscriber.next(curr + " ");
				}

				if (globalCounter.get() > numOfItems) {
					subscriber.complete();
					return;
				}
			}
		})
		                           .log("oooo")
		                           .flatMap((s) -> Flux.create((sub) -> timer.schedule(new TimerTask() {
			                           @Override
			                           public void run() {
				                           sub.next(s);
				                           sub.complete();
			                           }
		                           }, 10)))
		                           .log()
		                           .take(numOfItems + 1)
		                           .collectList();

		c.block();
	}
}

