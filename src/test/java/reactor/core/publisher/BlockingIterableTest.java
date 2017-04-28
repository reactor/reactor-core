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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;

public class BlockingIterableTest {

	@Test(timeout = 5000)
	public void normal() {
		List<Integer> values = new ArrayList<>();

		for (Integer i : Flux.range(1, 10)
		                     .toIterable()) {
			values.add(i);
		}

		Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
	}

	@Test(timeout = 5000)
	public void normal2() {
		Queue<Integer> q = new ArrayBlockingQueue<>(1);
		List<Integer> values = new ArrayList<>();

		for (Integer i : Flux.range(1, 10)
		                     .toIterable(1, () -> q)) {
			values.add(i);
		}

		Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
	}

	@Test(timeout = 5000)
	public void empty() {
		List<Integer> values = new ArrayList<>();

		for (Integer i : FluxEmpty.<Integer>instance().toIterable()) {
			values.add(i);
		}

		Assert.assertEquals(Collections.emptyList(), values);
	}

	@Test(timeout = 5000, expected = RuntimeException.class)
	public void error() {
		List<Integer> values = new ArrayList<>();

		for (Integer i : Flux.<Integer>error(new RuntimeException("forced failure")).toIterable()) {
			values.add(i);
		}

		Assert.assertEquals(Collections.emptyList(), values);
	}

	@Test(timeout = 5000)
	public void toStream() {
		List<Integer> values = new ArrayList<>();

		Flux.range(1, 10)
		    .toStream()
		    .forEach(values::add);

		Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
	}

	@Test(timeout = 5000)
	public void streamEmpty() {
		List<Integer> values = new ArrayList<>();

		FluxEmpty.<Integer>instance().toStream()
		                             .forEach(values::add);

		Assert.assertEquals(Collections.emptyList(), values);
	}

	@Test(timeout = 5000)
	public void streamLimit() {
		List<Integer> values = new ArrayList<>();

		Flux.range(1, Integer.MAX_VALUE)
		    .toStream()
		    .limit(10)
		    .forEach(values::add);

		Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), values);
	}

	@Test(timeout = 5000)
	public void streamParallel() {
		int n = 1_000_000;

		Optional<Integer> opt = Flux.range(1, n)
		                            .toStream()
		                            .parallel()
		                            .max(Integer::compare);

		Assert.assertTrue("No maximum?", opt.isPresent());
		Assert.assertEquals((Integer) n, opt.get());
	}

	private <V> void hotFluxToIterableOrStreamScenario(Function<Flux<Integer>, Publisher<V>> transformer,
								  BiConsumer<V, Consumer<Integer>> processor) throws InterruptedException {
		List<Integer> result = new ArrayList<>();
		CountDownLatch completeLatch = new CountDownLatch(1);
		CountDownLatch waitLatch = new CountDownLatch(1);
		Consumer<Object> wait = (x) -> {
			try {
				waitLatch.await();
			} catch (InterruptedException e) { /*ignore*/ }
		};
		ConnectableFlux<Integer> numbers = Flux.range(0, 1000).doOnNext(x -> waitLatch.countDown()).publish();
		numbers
				.transform(transformer)
				.publishOn(Schedulers.elastic())
				.doOnNext(wait)
				.doOnNext(l -> processor.accept(l, result::add))
				.doOnComplete(completeLatch::countDown)
				.subscribe();

		numbers.connect();
		completeLatch.await();
		Assert.assertArrayEquals(Flux.range(0, 1000).toStream().toArray(), result.toArray());
	}

	@Test(timeout = 5000)
	public void hotFluxToStream() throws InterruptedException {
		hotFluxToIterableOrStreamScenario(flux -> Flux.just(flux.toStream()), Stream::forEach);
	}

	@Test(timeout = 5000)
	public void hotFluxToIterable() throws InterruptedException {
		hotFluxToIterableOrStreamScenario(flux -> Flux.just(flux.toIterable()), Iterable::forEach);
	}
}
