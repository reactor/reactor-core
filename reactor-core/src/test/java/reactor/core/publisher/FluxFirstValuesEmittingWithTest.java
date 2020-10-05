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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.test.subscriber.AssertSubscriber;

public class FluxFirstValuesEmittingWithTest {

	@Test
	public void noStackOverflow() {
		int n = 5000;
		Flux<Integer> source = Flux.just(1);
		Flux<Integer> result = source;
		for (int i = 0; i < n; i++) {
			result = result.orValues(source);
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

		Flux.firstValues(Flux.just(1), Flux.just(2)).orValues(Flux.just(3))
				.subscribe(ts);
		ts.assertValues(1)
				.assertNoError()
				.assertComplete();
	}

	@Test
	public void pairWise() {
		Flux<Integer> f = Flux.firstValues(Mono.just(1), Mono.just(2))
				.orValues(Mono.just(3));

		Assertions.assertTrue(f instanceof FluxFirstValuesEmitting);
		FluxFirstValuesEmitting<Integer> s = (FluxFirstValuesEmitting<Integer>) f;
		Assertions.assertNotNull(s.array);
		Assertions.assertEquals(s.array.length, 3);

	}

	@Test
	public void pairWiseIterable() {
		Flux<Integer> f = Flux.firstValues(Arrays.asList(Mono.just(1), Mono.just(2)))
				.orValues(Mono.just(3));

		Assertions.assertTrue(f instanceof FluxFirstValuesEmitting);
		FluxFirstValuesEmitting<Integer> s = (FluxFirstValuesEmitting<Integer>) f;
		Assertions.assertNotNull(s.array);
		Assertions.assertEquals(s.array.length, 2);

	}
}