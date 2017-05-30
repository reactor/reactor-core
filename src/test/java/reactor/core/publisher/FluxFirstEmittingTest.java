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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxFirstEmittingTest {

	@Test(expected = NullPointerException.class)
	public void arrayNull() {
		Flux.firstEmitting((Publisher<Integer>[]) null);
	}

	@Test(expected = NullPointerException.class)
	public void iterableNull() {
		new FluxFirstEmitting<>((Iterable<Publisher<Integer>>) null);
	}

	@Test
	public void firstWinner() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.firstEmitting(Flux.range(1, 10), Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void firstWinnerBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(5);

		Flux.firstEmitting(Flux.range(1, 10), Flux.range(11, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();
	}

	@Test
	public void secondWinner() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.firstEmitting(Flux.never(),
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

		Flux.firstEmitting(Flux.never(), Flux.<Integer>error(ex))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(ex.getClass());
	}

	@Test
	public void singleArrayNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstEmitting((Publisher<Object>) null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void arrayOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstEmitting(Flux.never(), null, Flux.never())
		    .subscribe
		  (ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void singleIterableNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstEmitting(Arrays.asList((Publisher<Object>) null))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void iterableOneIsNullSource() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.firstEmitting(Arrays.asList(Flux.never(),
				(Publisher<Object>) null,
				Flux.never()))
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

    @Test
    public void scanSubscriber() {
        Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFirstEmitting.RaceCoordinator<String> parent = new FluxFirstEmitting.RaceCoordinator<>(1);
        FluxFirstEmitting.FirstEmittingSubscriber<String> test = new FluxFirstEmitting.FirstEmittingSubscriber<>(actual, parent, 1);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        parent.cancelled = true;
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

    @Test
    public void scanRaceCoordinator() {
        Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxFirstEmitting.RaceCoordinator<String> parent = new FluxFirstEmitting.RaceCoordinator<>(1);
        FluxFirstEmitting.FirstEmittingSubscriber<String> test = new FluxFirstEmitting.FirstEmittingSubscriber<>(actual, parent, 1);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(parent.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        parent.cancelled = true;
        assertThat(parent.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
