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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxRetryTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxRetry<>(null, 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void timesInvalid() {
		Flux.never()
		    .retry(-1);
	}

	@Test
	public void zeroRetryNoError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .retry(0)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	final Flux<Integer> source = Flux.concat(Flux.range(1, 3),
			Flux.error(new RuntimeException("forced failure")));

	@Test
	public void zeroRetry() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry(0)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void oneRetry() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry(1)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void oneRetryBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(4);

		source.retry(1)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1)
		  .assertNotComplete()
		  .assertNoError();
	}

	@Test
	public void retryInfinite() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.retry()
		      .take(10)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void twoRetryNormal() {
		AtomicInteger i = new AtomicInteger();

		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .doOnNext(d -> {
			                        if(i.getAndIncrement() < 2)
				                        throw new RuntimeException("test");
		                        })
		                        .retry(2)
		                        .count())
		            .expectNext(3L)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void doOnNextFails() {
		Flux.just(1)
		    .doOnNext(new Consumer<Integer>() {
			    int i;

			    @Override
			    public void accept(Integer t) {
				    if (i++ < 2) {
					    throw new RuntimeException("test");
				    }
			    }
		    })
		    .retry(2)
		    .subscribeWith(AssertSubscriber.create())
		    .assertValues(1);
	}

	@Test
	public void onLastAssemblyOnce() {
		AtomicInteger onAssemblyCounter = new AtomicInteger();
		String hookKey = UUID.randomUUID().toString();
		try {
			Hooks.onLastOperator(hookKey, publisher -> {
				onAssemblyCounter.incrementAndGet();
				return publisher;
			});
			Mono.error(new IllegalStateException("boom"))
			    .retry(1)
			    .block();
		}
		catch (IllegalStateException ignored) {
			// ignore
		}
		finally {
			Hooks.resetOnLastOperator(hookKey);
		}

		assertThat(onAssemblyCounter).hasValue(1);
	}
}
