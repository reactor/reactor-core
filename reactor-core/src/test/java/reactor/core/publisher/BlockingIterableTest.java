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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.Scannable.IntAttr;
import reactor.core.Scannable.ScannableAttr;
import reactor.util.concurrent.Queues;

import static org.assertj.core.api.Java6Assertions.assertThat;

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

	@Test
	public void scanOperator() {
		Flux<Integer> source = Flux.range(1, 10);
		BlockingIterable<Integer> test = new BlockingIterable<>(source, 35, Queues.one());

		assertThat(test.scanUnsafe(ScannableAttr.PARENT)).describedAs("PARENT").isSameAs(source);

		//type safe attributes
		assertThat(test.scanUnsafe(IntAttr.PREFETCH)).describedAs("PREFETCH unsafe").isEqualTo(35);
		assertThat(test.scan(IntAttr.PREFETCH)).describedAs("PREFETCH").isEqualTo(35); //FIXME
	}

	@Test
	public void scanOperatorLargePrefetchIsLimitedToIntMax() {
		Flux<Integer> source = Flux.range(1, 10);
		BlockingIterable<Integer> test = new BlockingIterable<>(source,
				Integer.MAX_VALUE + 30L,
				Queues.one());

		assertThat(test.scan(IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE); //FIXME
	}

	@Test
	public void scanSubscriber() {
		BlockingIterable.SubscriberIterator<String> subscriberIterator =
				new BlockingIterable.SubscriberIterator<>(Queues.<String>one().get(), 123);
		Subscription s = Operators.emptySubscription();
		subscriberIterator.onSubscribe(s);

		assertThat(subscriberIterator.scan(ScannableAttr.PARENT)).describedAs("PARENT").isSameAs(s);
		assertThat(subscriberIterator.scan(Scannable.BooleanAttr.TERMINATED)).describedAs("TERMINATED").isFalse();
		assertThat(subscriberIterator.scan(Scannable.BooleanAttr.CANCELLED)).describedAs("CANCELLED").isFalse();
		assertThat(subscriberIterator.scan(Scannable.ThrowableAttr.ERROR)).describedAs("ERROR").isNull();

		assertThat(subscriberIterator.scan(IntAttr.PREFETCH)).describedAs("PREFETCH").isEqualTo(123); //FIXME
	}

	@Test
	public void scanSubscriberLargePrefetchIsLimitedToIntMax() {
		BlockingIterable.SubscriberIterator<String> subscriberIterator =
				new BlockingIterable.SubscriberIterator<>(Queues.<String>one().get(),
						Integer.MAX_VALUE + 30L);

		assertThat(subscriberIterator.scan(IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE); //FIXME
	}

	@Test
	public void scanSubscriberTerminated() {
		BlockingIterable.SubscriberIterator<String> test =
				new BlockingIterable.SubscriberIterator<>(Queues.<String>one().get(), 123);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).describedAs("before TERMINATED").isFalse();

		test.onComplete();

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).describedAs("after TERMINATED").isTrue();
	}

	@Test
	public void scanSubscriberError() {
		BlockingIterable.SubscriberIterator<String> test = new BlockingIterable.SubscriberIterator<>(Queues.<String>one().get(),
				123);
		IllegalStateException error = new IllegalStateException("boom");

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).describedAs("before ERROR")
		                                                    .isNull();

		test.onError(error);

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).describedAs("after ERROR")
		                                                       .isSameAs(error);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanSubscriberCancelled() {
		BlockingIterable.SubscriberIterator<String> test = new BlockingIterable.SubscriberIterator<>(Queues.<String>one().get(),
				123);

		//simulate cancellation by offering two elements
		test.onNext("a");
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).describedAs("before CANCELLED").isFalse();

		test.onNext("b");
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).describedAs("after CANCELLED").isTrue();
	}
}
