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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import reactor.test.subscriber.AssertSubscriber;

public class FluxFirstEmittingWithTest {

	@Test
	public void noStackOverflow() {
		int n = 5000;
		
		Flux<Integer> source = Flux.just(1);
		
		Flux<Integer> result = source;
		
		for (int i = 0; i < n; i++) {
			result = result.or(source);
		}
		
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		result.subscribe(ts);
		
		ts.assertValues(1)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void dontBreakAmb() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		
		Flux.first(Flux.just(1), Flux.just(2)).or(Flux.just(3))
		    .subscribe(ts);

		
		ts.assertValues(1)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void pairWise() {
		Flux<Integer> f = Flux.first(Mono.just(1), Mono.just(2))
		                      .or(Mono.just(3));

		Assert.assertTrue(f instanceof FluxFirstEmitting);
		FluxFirstEmitting<Integer> s = (FluxFirstEmitting<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 3);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}

	@Test
	public void pairWiseIterable() {
		Flux<Integer> f = Flux.first(Arrays.asList(Mono.just(1), Mono.just(2)))
		                      .or(Mono.just(3));

		Assert.assertTrue(f instanceof FluxFirstEmitting);
		FluxFirstEmitting<Integer> s = (FluxFirstEmitting<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}
}