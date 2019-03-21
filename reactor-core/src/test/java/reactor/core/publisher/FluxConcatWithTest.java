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

import org.junit.Test;

import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

public class FluxConcatWithTest {

	@Test
	public void noStackOverflow() {
		int n = 5000;
		
		Flux<Integer> source = Flux.just(1);
		
		Flux<Integer> result = source;
		
		for (int i = 0; i < n; i++) {
			result = result.concatWith(source);
		}
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		result.subscribe(ts);
		
		ts.assertValueCount(n + 1)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void noStackOverflow2() {
		int n = 5000;
		
		Flux<Integer> source = Flux.just(1, 2).concatMap(Flux::just);
		Flux<Integer> add = Flux.just(3);
		
		Flux<Integer> result = source;
		
		for (int i = 0; i < n; i++) {
			result = result.concatWith(add);
		}
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		result.subscribe(ts);
		
		ts.assertValueCount(n + 2)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void noStackOverflow3() {
		int n = 5000;
		
		Flux<Flux<Integer>> source = Flux.just(Flux.just(1), Flux.just(2));
		Flux<Flux<Integer>> add = Flux.just(Flux.just(3));
		
		Flux<Flux<Integer>> result = source;
		
		for (int i = 0; i < n; i++) {
			result = result.concatWith(add);
		}
		
		AssertSubscriber<Object> ts = AssertSubscriber.create();
		
		result.subscribe(ts);
		
		ts.assertValueCount(n + 2)
		.assertNoError()
		.assertComplete();
	}

	
	@Test
	public void dontBreakFluxArrayConcatMap() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux.just(1, 2).concatMap(Flux::just).concatWith(Flux.just(3))
		.subscribe(ts);

		
		ts.assertValues(1, 2, 3)
		.assertNoError()
		.assertComplete();
	}

	@Test
    public void concatWithValues() {
      StepVerifier.create(Flux.just(1, 2).concatWithValues(4, 5, 6))
          .expectNext(1, 2, 4, 5, 6)
          .verifyComplete();
    }
}