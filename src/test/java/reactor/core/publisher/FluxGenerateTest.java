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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

public class FluxGenerateTest {

	@Test(expected = NullPointerException.class)
	public void stateSupplierNull() {
		Flux.generate(null, (s, o) -> s, s -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void generatorNull() {
		Flux.generate(() -> 1, null, s -> {
		});
	}

	@Test(expected = NullPointerException.class)
	public void stateConsumerNull() {
		Flux.generate(() -> 1, (s, o) -> s, null);
	}

	@Test
	public void generateEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.complete();
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.next(1);
			o.complete();
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.error(new RuntimeException("forced failure"));
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void generateJustBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>generate(o -> {
			o.next(1);
			o.complete();
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateRange() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer, Integer>generate(() -> 1, (s, o) -> {
			if (s < 11) {
				o.next(s);
			}
			else {
				o.complete();
			}
			return s + 1;
		}).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void generateRangeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer, Integer>generate(() -> 1, (s, o) -> {
			if (s < 11) {
				o.next(s);
			}
			else {
				o.complete();
			}
			return s + 1;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void stateSupplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer, Integer>generate(() -> {
			throw new RuntimeException("forced failure");
		}, (s, o) -> {
			o.next(1);
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class);
	}

	@Test
	public void generatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void generatorMultipleOnErrors() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.error(new RuntimeException("forced failure"));
			o.error(new RuntimeException("forced failure"));
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void generatorMultipleOnCompletes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.complete();
			o.complete();
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void generatorMultipleOnNexts() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>generate(o -> {
			o.next(1);
			o.next(1);
		}).subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(IllegalStateException.class);
	}

	@Test
	public void stateConsumerCalled() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger stateConsumer = new AtomicInteger();

		Flux.<Integer, Integer>generate(() -> 1, (s, o) -> {
			o.complete();
			return s;
		}, stateConsumer::set).subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();

		Assert.assertEquals(1, stateConsumer.get());
	}

	@Test
	public void iterableSource() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(list::iterator, (s, o) -> {
			if (s.hasNext()) {
				o.next(s.next());
			}
			else {
				o.complete();
			}
			return s;
		}).subscribe(ts);

		ts.assertValueSequence(list)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iterableSourceBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(list::iterator, (s, o) -> {
			if (s.hasNext()) {
				o.next(s.next());
			}
			else {
				o.complete();
			}
			return s;
		}).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);
		ts.assertValueSequence(list)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void fusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(() -> list.iterator(), (s, o) -> {
			if (s.hasNext()) {
				o.next(s.next());
			}
			else {
				o.complete();
			}
			return s;
		}).subscribe(ts);

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.SYNC)
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}

	@Test
	public void fusionBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY | Fuseable.THREAD_BARRIER);

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		Flux.<Integer, Iterator<Integer>>generate(list::iterator, (s, o) -> {
			if (s.hasNext()) {
				o.next(s.next());
			}
			else {
				o.complete();
			}
			return s;
		}).subscribe(ts);

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.NONE)
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
	}


    @Test
    public void scanSubscription() {
        Subscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxGenerate.GenerateSubscription<Integer, Integer> test =
                new FluxGenerate.GenerateSubscription<>(subscriber, 1, (s, o) -> null, s -> {});

        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(subscriber);
        test.request(5);
        assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(5L);
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
    }

    @Test
    public void scanSubscriptionError() {
        Subscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxGenerate.GenerateSubscription<Integer, Integer> test =
                new FluxGenerate.GenerateSubscription<>(subscriber, 1, (s, o) -> null, s -> {});

        test.error(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
        assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isNull();
    }

    @Test
    public void scanSubscriptionCancelled() {
        Subscriber<Integer> subscriber = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxGenerate.GenerateSubscription<Integer, Integer> test =
                new FluxGenerate.GenerateSubscription<>(subscriber, 1, (s, o) -> null, s -> {});

        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

}
