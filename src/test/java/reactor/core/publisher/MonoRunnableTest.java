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

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoRunnableTest {

	@Test(expected = NullPointerException.class)
	public void nullValue() {
		new MonoRunnable(null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Void> ts = AssertSubscriber.create();

		Mono.fromRunnable(() -> {
		})
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Void> ts = AssertSubscriber.create(0);

		Mono.fromRunnable(() -> {
		})
		    .hide()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void asyncRunnable() {
		AtomicReference<Thread> t = new AtomicReference<>();
		StepVerifier.create(Mono.fromRunnable(() -> t.set(Thread.currentThread()))
		                        .subscribeOn(Schedulers.single()))
		            .verifyComplete();

		assertThat(t).isNotNull();
		assertThat(t).isNotEqualTo(Thread.currentThread());
	}

	@Test
	public void asyncRunnableBackpressured() {
		AtomicReference<Thread> t = new AtomicReference<>();
		StepVerifier.create(Mono.fromRunnable(() -> t.set(Thread.currentThread()))
		                        .subscribeOn(Schedulers.single()), 0)
		            .verifyComplete();

		assertThat(t).isNotNull();
		assertThat(t).isNotEqualTo(Thread.currentThread());
	}

	@Test
	public void runnableThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.fromRunnable(() -> {
			throw new RuntimeException("forced failure");
		})
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void nonFused() {
		AssertSubscriber<Void> ts = AssertSubscriber.create();

		Mono.fromRunnable(() -> {
		})
		    .subscribe(ts);

		ts.assertNonFuseableSource()
		  .assertNoValues();
	}

	@Test
	public void test() {
		int c[] = { 0 };
		Flux.range(1, 1000)
		    .flatMap(v -> Mono.fromRunnable(() -> { c[0]++; }))
		    .ignoreElements()
		    .block();

		Assert.assertEquals(1000, c[0]);
	}
}
