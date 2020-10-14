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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoCallableTest {

	@Test
	public void nullCallable() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.<Integer>fromCallable(null);
		});
	}

	@Test
	public void callableReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.<Integer>fromCallable(() -> null).subscribe(ts);

		ts.assertNoValues()
				.assertNoError()
				.assertComplete();
	}

	@Test
	public void callableReturnsNullShortcircuitsBackpressure() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.<Integer>fromCallable(() -> null).subscribe(ts);

		ts.assertNoValues()
				.assertNoError()
				.assertComplete();
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Mono.fromCallable(() -> 1).subscribe(ts);

		ts.assertValues(1)
				.assertComplete()
				.assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Mono.fromCallable(() -> 1).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertNoError();

		ts.request(1);

		ts.assertValues(1)
				.assertComplete()
				.assertNoError();
	}

	@Test
	public void callableThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.fromCallable(() -> {
			throw new IOException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(IOException.class)
				.assertErrorMessage("forced failure");
	}

	@Test
	public void onMonoSuccessCallableOnBlock() {
		assertThat(Mono.fromCallable(() -> "test")
				.block()).isEqualToIgnoringCase("test");
	}

	@Test
	public void onMonoEmptyCallableOnBlock() {
		assertThat(Mono.fromCallable(() -> null)
				.block()).isNull();
	}

	@Test
	public void onMonoErrorCallableOnBlock() {
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> {
			Mono.fromCallable(() -> {
				throw new Exception("test");
			}).block();
		});
	}

	@Test
	public void delegateCall() throws Exception {
		MonoCallable<Integer> monoCallable = new MonoCallable<>(() -> 1);

		assertThat(monoCallable.call()).isEqualTo(1);
	}

	@Test
	public void delegateCallError() {
		MonoCallable<Integer> monoCallable = new MonoCallable<>(() -> {
			throw new IllegalStateException("boom");
		});

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(monoCallable::call)
				.withMessage("boom");
	}

	@Test
	public void delegateCallNull() throws Exception {
		MonoCallable<Integer> monoCallable = new MonoCallable<>(() -> null);

		assertThat(monoCallable.call()).isNull();
	}

	//see https://github.com/reactor/reactor-core/issues/1503
	@Test
	public void callableCancelledBeforeRun() {
		AtomicBoolean actual = new AtomicBoolean(true);
		Mono<?> mono = Mono.fromCallable(() -> actual.getAndSet(false))
				.doOnSubscribe(Subscription::cancel);

		StepVerifier.create(mono)
				.expectSubscription()
				.expectNoEvent(Duration.ofSeconds(1))
				.thenCancel()
				.verify();

		assertThat(actual).as("cancelled before run").isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/1503
	//see https://github.com/reactor/reactor-core/issues/1504
	@Test
	public void callableSubscribeToCompleteMeasurement() {
		AtomicLong subscribeTs = new AtomicLong();
		Mono<String> mono = Mono.fromCallable(() -> {
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			return "foo";
		})
				.doOnSubscribe(sub -> subscribeTs.set(-1 * System.nanoTime()))
				.doFinally(fin -> subscribeTs.addAndGet(System.nanoTime()));

		StepVerifier.create(mono)
				.expectNext("foo")
				.verifyComplete();

        assertThat(TimeUnit.NANOSECONDS.toMillis(subscribeTs.get())).isCloseTo(500L, Offset.offset(50L));
    }

    @Test
    public void scanOperator(){
        MonoCallable<Integer> test = new MonoCallable<>(() -> 1);

        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }
}
