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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;

public class MonoRetryTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoRetry<>(null, 1);
		});
	}

	@Test
	public void timesInvalid() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Mono.never().retry(-1);
		});
	}

	@Test
	public void zeroRetryNoError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.just(1)
		    .retry(0)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void zeroRetry() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>error(new RuntimeException("forced failure")).retry(0)
		                                                           .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void oneRetry() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(() -> {
			int _i = i.getAndIncrement();
			if (_i < 1) {
				throw Exceptions.propagate(new RuntimeException("forced failure"));
			}
			return _i;
		})
		    .retry(1)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void retryInfinite() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		AtomicInteger i = new AtomicInteger();
		Mono.fromCallable(() -> {
			int _i = i.getAndIncrement();
			if (_i < 10) {
				throw Exceptions.propagate(new RuntimeException("forced failure"));
			}
			return _i;
		})
		    .retry()
		    .subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void doOnNextFails() {
		Mono.just(1)
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
	public void scanOperator(){
	    MonoRetry<Integer> test = new MonoRetry<>(Mono.just(1), 3L);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}
