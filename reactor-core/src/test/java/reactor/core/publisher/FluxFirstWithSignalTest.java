/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxFirstWithSignalTest {

	@Test
	public void arrayNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.firstWithSignal((Publisher<Integer>[]) null);
		});
	}

	@Test
	public void iterableNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxFirstWithSignal<>((Iterable<Publisher<Integer>>) null);
		});
	}

	@Test
	public void firstWinner() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.firstWithSignal(Flux.range(1, 10), Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void firstWinnerBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(5);

		Flux.firstWithSignal(Flux.range(1, 10), Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();
	}

	@Test
	public void secondWinner() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.firstWithSignal(Flux.never(),
				Flux.range(11, 10)
				    .log())
		    .subscribe(ts);

		ts.assertValues(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void secondEmitsError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		RuntimeException ex = new RuntimeException("forced failure");

		Flux.firstWithSignal(Flux.never(), Flux.<Integer>error(ex))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(ex.getClass());
	}

	@Test
	public void singleArrayNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithSignal((Publisher<Object>) null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithSignal(Flux.never(), null, Flux.never())
		    .subscribe
		  (ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithSignal(Arrays.asList((Publisher<Object>) null))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstWithSignal(Arrays.asList(Flux.never(),
				(Publisher<Object>) null,
				Flux.never()))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void scanOperator(){
		@SuppressWarnings("unchecked") FluxFirstWithSignal<Integer>
				test = new FluxFirstWithSignal<>(Flux.range(1, 10), Flux.range(11, 10));

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

    @Test
    public void scanSubscriber() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFirstWithSignal.RaceCoordinator<String> parent = new FluxFirstWithSignal.RaceCoordinator<>(1);
        FluxFirstWithSignal.FirstEmittingSubscriber<String> test = new FluxFirstWithSignal.FirstEmittingSubscriber<>(actual, parent, 1);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        parent.cancelled = true;
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    public void scanRaceCoordinator() {
        CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFirstWithSignal.RaceCoordinator<String> parent = new FluxFirstWithSignal.RaceCoordinator<>(1);
        FluxFirstWithSignal.FirstEmittingSubscriber<String> test = new FluxFirstWithSignal.FirstEmittingSubscriber<>(actual, parent, 1);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(parent.scan(Scannable.Attr.CANCELLED)).isFalse();
        parent.cancelled = true;
        assertThat(parent.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
