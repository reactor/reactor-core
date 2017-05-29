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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.publisher.ReduceOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class MonoReduceTest extends ReduceOperatorTest<String, String>{

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.receive(1, i -> item(0));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.reduce((a, b) -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.reduce((a, b) -> null))
		);
	}

	private static final Logger LOG = LoggerFactory.getLogger(MonoReduceTest.class);

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(MonoReduce.class);

		ctb.addRef("source", Mono.never());
		ctb.addRef("aggregator", (BiFunction<Object, Object, Object>) (a, b) -> b);

		ctb.test();
	}
*/
	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduce((a, b) -> a + b)
		    .subscribe(ts);

		ts.assertValues(55)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0L);

		Flux.range(1, 10)
		    .reduce((a, b) -> a + b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(55)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void single() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1)
		    .reduce((a, b) -> a + b)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().reduce((a, b) -> a + b)
		                     .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>error(new RuntimeException("forced failure")).reduce((a, b) -> a + b)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure")))
		  .assertNotComplete();
	}

	@Test
	public void aggregatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduce((a, b) -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorWith(e -> Assert.assertTrue(e.getMessage()
		                                           .contains("forced failure")))
		  .assertNotComplete();
	}

	@Test
	public void aggregatorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduce((a, b) -> null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	/* see issue #230 */
	@Test
	public void should_reduce_to_10_events() {
		AtomicInteger count = new AtomicInteger();
		AtomicInteger countNulls = new AtomicInteger();
		Flux.range(0, 10).flatMap(x ->
				Flux.range(0, 2)
				    .map(y -> blockingOp(x, y))
				    .subscribeOn(Schedulers.elastic())
				    .reduce((l, r) -> l + "_" + r)
//				    .log("reduced."+x)
				    .doOnSuccess(s -> {
					    if (s == null) countNulls.incrementAndGet();
					    else count.incrementAndGet();
					    LOG.info("Completed with {}", s);
				    })
		).blockLast();

		assertEquals(10, count.get());
		assertEquals(0, countNulls.get());
	}

	private static String blockingOp(Integer x, Integer y) {
		try {
			sleep(1000 - x * 100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "x" + x + "y" + y;
	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoReduce.ReduceSubscriber<String> test = new MonoReduce.ReduceSubscriber<>(actual, (s1, s2) -> s1 + s2);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

}
