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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxOnBackpressureLatestTest {

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxOnBackpressureLatest<>(null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).onBackpressureLatest().subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void backpressuredComplex() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10000000)
		    .subscribeOn(Schedulers.parallel())
		    .onBackpressureLatest()
		    .publishOn(Schedulers.single())
		    .concatMap(Mono::just, 1)
		    .subscribe(ts);

		for (int i = 0; i < 1000000; i++) {
			ts.request(10);
		}

		ts.await();

		ts
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void backpressured() {
		Sinks.Many<Integer> tp = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		tp.asFlux().onBackpressureLatest().subscribe(ts);

		tp.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		tp.emitNext(2, FAIL_FAST);

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

		tp.emitNext(3, FAIL_FAST);
		tp.emitNext(4, FAIL_FAST);

		ts.request(2);

		ts.assertValues(2, 4)
		  .assertNoError()
		  .assertNotComplete();

		tp.emitNext(5, FAIL_FAST);
		tp.emitComplete(FAIL_FAST);

		ts.assertValues(2, 4, 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void error() {
		Sinks.Many<Integer> tp = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		tp.asFlux().onBackpressureLatest().subscribe(ts);

		tp.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void backpressureWithDrop() {
		Sinks.Many<Integer> tp = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>(0) {
			@Override
			public void onNext(Integer t) {
				super.onNext(t);
				if (t == 2) {
					tp.emitNext(3, FAIL_FAST);
				}
			}
		};

		tp.asFlux()
		  .onBackpressureLatest()
		  .subscribe(ts);

		tp.emitNext(1, FAIL_FAST);
		tp.emitNext(2, FAIL_FAST);

		ts.request(1);

		ts.assertValues(2)
		  .assertNoError()
		  .assertNotComplete();

	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxOnBackpressureLatest<Integer> test = new FluxOnBackpressureLatest<>(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureLatest.LatestSubscriber<Integer> test =
        		new FluxOnBackpressureLatest.LatestSubscriber<>(actual);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        test.value = 9;
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
