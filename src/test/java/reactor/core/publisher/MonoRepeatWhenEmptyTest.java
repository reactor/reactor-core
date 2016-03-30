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

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;

import reactor.core.test.TestSubscriber;

public class MonoRepeatWhenEmptyTest {
	@Test
	public void repeatInfinite() {
		AtomicInteger c = new AtomicInteger();
		
		Mono<Integer> source = Mono.defer(
		        () -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just(c.get()));

		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		source.repeatWhenEmpty(o -> o).subscribe(ts);
		
		Assert.assertEquals(4, c.get());
		
		ts.assertValues(4)
		.assertComplete()
		.assertNoError();
	}


	@Test
	public void repeatFinite() {
		AtomicInteger c = new AtomicInteger();
		
		Mono<Integer> source = Mono.defer(
		        () -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just(c.get()));

		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		source.repeatWhenEmpty(1000, o -> o).subscribe(ts);
		
		Assert.assertEquals(4, c.get());
		
		ts.assertValues(4)
		.assertComplete()
		.assertNoError();
	}

	@Test
	public void repeatFiniteLessTimes() {
		AtomicInteger c = new AtomicInteger();
		
		Mono<Integer> source = Mono.defer(
		        () -> c.getAndIncrement() < 3 ? Mono.empty() : Mono.just(c.get()));

		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		source.repeatWhenEmpty(2, o -> o).subscribe(ts);
		
		Assert.assertEquals(3, c.get());
		
		ts.assertNoValues()
		.assertNotComplete()
		.assertError(NoSuchElementException.class);
	}
}
