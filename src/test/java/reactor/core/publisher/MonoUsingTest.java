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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.test.TestSubscriber;

public class MonoUsingTest {

	@Test(expected = NullPointerException.class)
	public void resourceSupplierNull() {
		Mono.using(null, r -> Mono.empty(), r -> {
		}, false);
	}

	@Test(expected = NullPointerException.class)
	public void sourceFactoryNull() {
		Mono.using(() -> 1, null, r -> {
		}, false);
	}

	@Test(expected = NullPointerException.class)
	public void resourceCleanupNull() {
		Mono.using(() -> 1, r -> Mono.empty(), null, false);
	}

	@Test
	public void normal() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> Mono.just(1), cleanup::set, false)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, cleanup.get());
	}

	@Test
	public void normalEager() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> Mono.just(1), cleanup::set, true)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, cleanup.get());
	}

	void checkCleanupExecutionTime(boolean eager, boolean fail) {
		AtomicInteger cleanup = new AtomicInteger();
		AtomicBoolean before = new AtomicBoolean();

		TestSubscriber<Integer> ts = new TestSubscriber<Integer>() {
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

		Mono.using(() -> 1, r -> {
			if (fail) {
				return Mono.error(new RuntimeException("forced failure"));
			}
			return Mono.just(1);
		}, cleanup::set, eager)
		    .subscribe(ts);

		if (fail) {
			ts.assertNoValues()
			  .assertError(RuntimeException.class)
			  .assertNotComplete()
			  .assertErrorMessage("forced failure");
		}
		else {
			ts.assertValues(1)
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
		TestSubscriber<Object> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> {
			throw new RuntimeException("forced failure");
		}, r -> Mono.just(1), cleanup::set, false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		Assert.assertEquals(0, cleanup.get());
	}

	@Test
	public void factoryThrowsEager() {
		TestSubscriber<Object> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> {
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
		TestSubscriber<Object> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.<Integer, Integer>using(() -> 1,
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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		MonoProcessor<Integer> tp = MonoProcessor.create();

		Mono.using(() -> 1, r -> tp, cleanup::set, true)
		    .subscribe(ts);

		Assert.assertTrue("No subscriber?", tp.hasDownstreams());

		tp.onNext(1);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();


		Assert.assertEquals(1, cleanup.get());
	}

}
