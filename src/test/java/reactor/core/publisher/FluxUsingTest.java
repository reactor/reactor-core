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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxUsingTest extends AbstractFluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY)
				.shouldHitDropNextHookAfterTerminate(false)
				.shouldHitDropErrorHookAfterTerminate(false);
	}

	@Override
	protected int fusionModeThreadBarrierSupport() {
		return Fuseable.ANY;
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorInOperatorCallback() {
		return Arrays.asList(
				scenario(f -> Flux.using(() -> {
					throw exception();
				}, c -> f, c -> {}))
						.fusionMode(Fuseable.NONE),

				scenario(f -> Flux.using(() -> 0, c -> null, c -> {}))
						.fusionMode(Fuseable.NONE)
						.verifier(step -> step.verifyError(NullPointerException.class)),

				scenario(f -> Flux.using(() -> 0, c -> {
					throw exception();
				}, c -> {}))
						.fusionMode(Fuseable.NONE)

				/*scenario(f -> Flux.using(() -> 0, c -> f, c -> {
					throw exception();
				}))
				.verifier(step -> {
					try {
						step.expectNext(item(0), item(1), item(2)).verifyErrorMessage
								("test");
					}
					catch (Exception t){
						assertThat(Exceptions.unwrap(t)).hasMessage("test");
					}
				})*/

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_threeNextAndComplete() {
		return Arrays.asList(
				scenario(f -> Flux.using(() -> 0, c -> f, c -> {})),

				scenario(f -> Flux.using(() -> 0, c -> f, c -> {}, false))
		);
	}

	@Test(expected = NullPointerException.class)
	public void resourceSupplierNull() {
		Flux.using(null, r -> Flux.empty(), r -> {
		}, false);
	}

	@Test(expected = NullPointerException.class)
	public void sourceFactoryNull() {
		Flux.using(() -> 1, null, r -> {
		}, false);
	}

	@Test(expected = NullPointerException.class)
	public void resourceCleanupNull() {
		Flux.using(() -> 1, r -> Flux.empty(), null, false);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set, false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, cleanup.get());
	}

	@Test
	public void normalEager() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, cleanup.get());
	}

	void checkCleanupExecutionTime(boolean eager, boolean fail) {
		AtomicInteger cleanup = new AtomicInteger();
		AtomicBoolean before = new AtomicBoolean();

		AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>() {
			@Override
			public void onError(Throwable t) {
				super.onError(t);
				before.set(cleanup.get() != 0);
			}

			@Override
			public void onComplete() {
				super.onComplete();
				before.set(cleanup.get() != 0);
			}
		};

		Flux.using(() -> 1, r -> {
			if (fail) {
				return Flux.error(new RuntimeException("forced failure"));
			}
			return Flux.range(r, 10);
		}, cleanup::set, eager)
		    .subscribe(ts);

		if (fail) {
			ts.assertNoValues()
			  .assertError(RuntimeException.class)
			  .assertNotComplete()
			  .assertErrorMessage("forced failure");
		}
		else {
			ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			  .assertComplete()
			  .assertNoError();
		}

		Assert.assertEquals(1, cleanup.get());
		Assert.assertEquals(eager, before.get());
	}

	@Test
	public void checkNonEager() {
		checkCleanupExecutionTime(false, false);
	}

	@Test
	public void checkEager() {
		checkCleanupExecutionTime(true, false);
	}

	@Test
	public void checkErrorNonEager() {
		checkCleanupExecutionTime(false, true);
	}

	@Test
	public void checkErrorEager() {
		checkCleanupExecutionTime(true, true);
	}

	@Test
	public void resourceThrowsEager() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> {
			throw new RuntimeException("forced failure");
		}, r -> Flux.range(1, 10), cleanup::set, false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		Assert.assertEquals(0, cleanup.get());
	}

	@Test
	public void factoryThrowsEager() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> 1, r -> {
			throw new RuntimeException("forced failure");
		}, cleanup::set, false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		Assert.assertEquals(1, cleanup.get());
	}

	@Test
	public void factoryReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.<Integer, Integer>using(() -> 1,
				r -> null,
				cleanup::set,
				false).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);

		Assert.assertEquals(1, cleanup.get());
	}

	@Test
	public void subscriberCancels() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		DirectProcessor<Integer> tp = DirectProcessor.create();

		Flux.using(() -> 1, r -> tp, cleanup::set, true)
		    .subscribe(ts);

		Assert.assertTrue("No subscriber?", tp.hasDownstreams());

		tp.onNext(1);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertNoError();

		ts.cancel();

		tp.onNext(2);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertNoError();

		Assert.assertFalse("Has subscriber?", tp.hasDownstreams());

		Assert.assertEquals(1, cleanup.get());
	}

}
