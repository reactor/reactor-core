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

import java.util.function.Consumer;

import org.junit.Test;
import reactor.test.TestSubscriber;

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

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
		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.retry(0)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void oneRetry() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.retry(1)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void oneRetryBackpressured() {
		TestSubscriber<Integer> ts = TestSubscriber.create(4);

		source.retry(1)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1)
		  .assertNotComplete()
		  .assertNoError();
	}

	@Test
	public void retryInfinite() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		source.retry()
		      .take(10)
		      .subscribe(ts);

		ts.assertValues(1, 2, 3, 1, 2, 3, 1, 2, 3, 1)
		  .assertComplete()
		  .assertNoError();

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
		    .subscribeWith(TestSubscriber.create())
		    .assertValues(1);
	}
}
